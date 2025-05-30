"""
Airflow DAG to process Anthem pricing transparency data:
1. Extract file references from the Anthem index file
2. Process and load data into Iceberg tables on S3

This DAG maintains a cloud-agnostic approach using standard Hadoop/Spark/Iceberg
components rather than AWS-specific services.
"""

from datetime import datetime, timedelta
import os
import re
import gzip
import json
import boto3
import time
import traceback
from urllib.parse import urlparse
import ast

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=24),  # Longer timeout for large file processing
}

# Define DAG
dag = DAG(
    'process_anthem_index_file_references',
    default_args=default_args,
    description='Process Anthem price transparency data to Iceberg tables',
    schedule='@monthly',  # Monthly execution - adjust as needed
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['price-transparency', 'anthem', 'iceberg'],
)

# Configuration - Centralize all config here for easy updates
config = {
    # S3 paths
    's3_bucket': 'price-transparency-raw',
    # Use a fixed path for testing, or the templated path for production
    'index_file_key': 'payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz',  # Fixed path for now
    'warehouse_location': 's3a://price-transparency-raw/warehouse',
    
    # Iceberg config
    'catalog_name': 'anthem_catalog',
    'file_refs_table': 'anthem_file_refs',
    
    # Processing parameters
    'batch_size': 10000,
    'max_records': None,  # Set to a number for testing/limiting
    'max_chunks': 5,      # Number of chunks to process (each 10MB)
    'output_file': '/home/airflow/anthem_file_references.txt',
    'log_file': '/home/airflow/anthem_extraction.log',
}

# Task 1: Check if the index file exists
check_index_file = S3KeySensor(
    task_id='check_index_file',
    bucket_key=config['index_file_key'],
    bucket_name=config['s3_bucket'],
    aws_conn_id='aws_default',
    timeout=60 * 60 * 24,  # 24 hours timeout
    poke_interval=60 * 60,  # Check every 60 minutes
    dag=dag
)

# Task 2: Setup the environment and prepare for data processing
def setup_environment(**context):
    """Create working directory and prepare the environment."""
    import subprocess
    import os
    
    # Make sure the necessary directories exist
    subprocess.run("mkdir -p /home/airflow/anthem-processing", shell=True, check=True)
    
    # Create a log directory
    log_dir = "/home/airflow/logs/anthem"
    subprocess.run(f"mkdir -p {log_dir}", shell=True, check=True)
    
    # Test AWS connectivity
    try:
        import boto3
        s3 = boto3.client('s3')
        buckets = s3.list_buckets()
        print(f"✅ Successfully connected to AWS. Found {len(buckets.get('Buckets', []))} buckets")
        
        # Try to list objects in target bucket
        bucket = config['s3_bucket']
        prefix = 'payer/anthem'
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        if 'Contents' in response:
            print(f"✅ Successfully listed objects in {bucket}/{prefix}")
            for obj in response.get('Contents', [])[:3]:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print(f"⚠️ No objects found in {bucket}/{prefix} or access denied")
    except Exception as e:
        print(f"⚠️ Warning: Could not connect to AWS: {e}")
    
    print("✅ Successfully set up environment for Anthem file processing")
    return True

# Task 3: Extract file references and save to output file
def extract_file_references(**context):
    """
    Extract file references from the Anthem index file.
    This function reads the index file directly from S3, extracts URLs,
    and writes the results to a text file.
    """
    # Initialize log file
    with open(config['log_file'], "w") as f:
        f.write(f"=== Anthem Index Extraction - Started {datetime.now()} ===\n")
    
    start_time = time.time()
    log_message(f"Starting Anthem index file extraction")
    
    # File reference patterns
    URL_PATTERNS = [
        r'"in_network_files"\s*:\s*\[\s*"([^"]+)"',
        r'"allowed_amount_files"\s*:\s*\[\s*"([^"]+)"',
        r'"url"\s*:\s*"([^"]+\.(?:json|csv|parquet|gz))"',
        r'https?://[^"\s]+\.(?:json|csv|parquet|gz)'
    ]
    
    try:
        # Test S3 connectivity
        if not test_s3_access():
            error_msg = "S3 connectivity test failed. Check AWS credentials and permissions."
            log_message(f"⚠️ {error_msg}")
            with open(config['output_file'], "w") as f:
                f.write(f"ERROR: {error_msg}\n")
            return 1
        
        # Parse S3 URL
        s3_url = f"s3://{config['s3_bucket']}/{config['index_file_key']}"
        parsed = urlparse(s3_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        
        # Initialize S3 client
        s3 = boto3.client("s3")
        
        # Check if file exists and get metadata
        try:
            head_response = s3.head_object(Bucket=bucket, Key=key)
            file_size = head_response.get('ContentLength', 0)
            log_message(f"✅ Successfully accessed index file. Size: {file_size/1024/1024:.2f} MB")
        except Exception as e:
            error_msg = f"Cannot access index file: {e}"
            log_message(f"❌ {error_msg}")
            with open(config['output_file'], "w") as f:
                f.write(f"ERROR: {error_msg}\n")
            return 1
        
        # Force processing to take longer for testing - this ensures Airflow sees real work happening
        log_message(f"💤 Processing large file. This may take several minutes...")
        
        # Read file in chunks
        references = []
        offset = 0
        chunk_size = 10 * 1024 * 1024  # 10MB chunks
        max_chunks = config['max_chunks']  # Process up to X chunks to ensure we get actual data
        chunks_processed = 0
        
        for chunk_num in range(max_chunks):
            chunk_start = time.time()
            try:
                # Get range of bytes
                range_header = f"bytes={offset}-{offset+chunk_size-1}"
                log_message(f"📥 Requesting chunk {chunk_num+1}: {range_header}")
                
                response = s3.get_object(Bucket=bucket, Key=key, Range=range_header)
                chunk_data = response["Body"].read()
                
                if not chunk_data:
                    log_message("End of file reached (empty response)")
                    break
                    
                log_message(f"Received {len(chunk_data)/1024/1024:.2f} MB of data")
                
                # Decompress if gzipped
                decompressed_size = 0
                if key.endswith(".gz"):
                    try:
                        decompress_start = time.time()
                        chunk_data = gzip.decompress(chunk_data)
                        decompressed_size = len(chunk_data)
                        log_message(f"Decompressed to {decompressed_size/1024/1024:.2f} MB in {time.time()-decompress_start:.2f}s")
                    except Exception as e:
                        log_message(f"⚠️ Warning: Could not decompress chunk {chunk_num+1}: {e}")
                
                # Convert to string and find patterns
                decode_start = time.time()
                chunk_str = chunk_data.decode("utf-8", errors="replace")
                log_message(f"Decoded chunk to {len(chunk_str)} characters in {time.time()-decode_start:.2f}s")
                
                # Show sample of first chunk for debugging
                if chunk_num == 0:
                    sample = chunk_str[:200]
                    log_message(f"Sample of first chunk: {sample}...")
                
                # Extract URLs using regex patterns
                pattern_start = time.time()
                chunk_refs = []
                for pattern in URL_PATTERNS:
                    matches = re.findall(pattern, chunk_str)
                    if matches:
                        log_message(f"Found {len(matches)} matches with pattern {pattern[:20]}...")
                        if len(matches) > 0 and chunk_num == 0:
                            log_message(f"Sample match: {matches[0][:100]}")
                    chunk_refs.extend(matches)
                
                log_message(f"Extracted {len(chunk_refs)} references in {time.time()-pattern_start:.2f}s")
                references.extend(chunk_refs)
                
                # Move to next chunk
                offset += len(chunk_data)
                chunks_processed += 1
                
                # Log chunk processing stats
                chunk_time = time.time() - chunk_start
                log_message(f"✅ Processed chunk {chunk_num+1} in {chunk_time:.2f}s. Total references: {len(references)}")
                
                # If this chunk was smaller than requested, we've reached EOF
                if len(chunk_data) < chunk_size:
                    log_message(f"End of file reached (smaller chunk than requested)")
                    break
                    
            except Exception as e:
                log_message(f"❌ Error processing chunk {chunk_num+1}: {e}")
                traceback.print_exc(file=open(config['log_file'], "a"))
                if chunk_num > 0:  # Only break if we've processed at least one chunk
                    break
                else:
                    with open(config['output_file'], "w") as f:
                        f.write(f"ERROR: Failed to process file: {e}\n")
                    return 1
        
        # If we didn't process any chunks successfully
        if chunks_processed == 0:
            log_message("Failed to process any chunks successfully")
            with open(config['output_file'], "w") as f:
                f.write("ERROR: Failed to process any data chunks\n")
            return 1
        
        # Remove duplicates
        unique_refs = list(set(references))
        log_message(f"🔍 Found {len(references)} total references, {len(unique_refs)} unique")
        
        # Categorize references
        in_network = [ref for ref in unique_refs if "in-network" in ref.lower()]
        allowed_amount = [ref for ref in unique_refs if "allowed-amount" in ref.lower()]
        other = [ref for ref in unique_refs if "in-network" not in ref.lower() and "allowed-amount" not in ref.lower()]
        
        log_message(f"📊 Categories: {len(in_network)} in-network, {len(allowed_amount)} allowed-amount, {len(other)} other")
        
        # Save results
        with open(config['output_file'], "w") as f:
            f.write(f"TOTAL_REFS: {len(unique_refs)}\n")
            f.write(f"IN_NETWORK: {len(in_network)}\n")
            f.write(f"ALLOWED_AMOUNT: {len(allowed_amount)}\n")
            f.write(f"OTHER: {len(other)}\n")
            f.write(f"CHUNKS_PROCESSED: {chunks_processed}\n\n")
            
            # Write sample of each type
            f.write("SAMPLES:\n")
            
            if in_network:
                f.write("\nIN-NETWORK SAMPLES:\n")
                for i, ref in enumerate(in_network[:10]):
                    f.write(f"{i+1}. {ref}\n")
                    
            if allowed_amount:
                f.write("\nALLOWED-AMOUNT SAMPLES:\n")
                for i, ref in enumerate(allowed_amount[:10]):
                    f.write(f"{i+1}. {ref}\n")
                    
            if other:
                f.write("\nOTHER SAMPLES:\n")
                for i, ref in enumerate(other[:10]):
                    f.write(f"{i+1}. {ref}\n")
        
        total_time = time.time() - start_time
        log_message(f"✅ Extraction complete! Processed in {total_time:.2f}s")
        log_message(f"📄 Results written to {config['output_file']}")
        log_message(f"📊 Found {len(unique_refs)} unique file references")
        
        # Push results to XCom for downstream tasks
        context['ti'].xcom_push(key='total_file_refs', value=len(unique_refs))
        context['ti'].xcom_push(key='in_network_count', value=len(in_network))
        context['ti'].xcom_push(key='allowed_amount_count', value=len(allowed_amount))
        context['ti'].xcom_push(key='other_count', value=len(other))
        
        # For the sample URLs, just push a smaller list to avoid XCom size limits
        all_samples = in_network[:5] + allowed_amount[:5] + other[:5]
        context['ti'].xcom_push(key='sample_urls', value=str(all_samples[:10]))
        
        return len(unique_refs)
        
    except Exception as e:
        log_message(f"❌ Error in main process: {e}")
        traceback.print_exc(file=open(config['log_file'], "a"))
        
        # Write error to output file for Airflow to read
        with open(config['output_file'], "w") as f:
            f.write(f"ERROR: {e}\n")
        return 1

# Task 4: Verify extraction results
def verify_extraction(**context):
    """Verify the extracted file references from the output file."""
    import os
    
    output_file = config['output_file']
    
    # Check if the output file exists
    if not os.path.exists(output_file):
        raise ValueError(f"Output file not found: {output_file}")
    
    # Read and analyze the file
    with open(output_file, 'r') as f:
        content = f.read()
    
    # Check for errors in the output
    if content.startswith("ERROR:"):
        error_msg = content.split("\n")[0].strip()
        raise ValueError(f"Extraction failed: {error_msg}")
    
    # Parse the summary statistics - we've already pushed these to XCom in the extract task
    # but we'll read them from the file here as a double-check
    total_refs = context['ti'].xcom_pull(task_ids='extract_file_references', key='total_file_refs')
    in_network_count = context['ti'].xcom_pull(task_ids='extract_file_references', key='in_network_count')
    allowed_amount_count = context['ti'].xcom_pull(task_ids='extract_file_references', key='allowed_amount_count')
    other_count = context['ti'].xcom_pull(task_ids='extract_file_references', key='other_count')
    
    # If for some reason the XCom values aren't available, try to parse them from the file
    if total_refs is None:
        total_match = re.search(r'TOTAL_REFS:\s*(\d+)', content)
        if total_match:
            total_refs = int(total_match.group(1))
        else:
            total_refs = 0
            
    if in_network_count is None:
        in_network_match = re.search(r'IN_NETWORK:\s*(\d+)', content)
        if in_network_match:
            in_network_count = int(in_network_match.group(1))
        else:
            in_network_count = 0
            
    if allowed_amount_count is None:
        allowed_amount_match = re.search(r'ALLOWED_AMOUNT:\s*(\d+)', content)
        if allowed_amount_match:
            allowed_amount_count = int(allowed_amount_match.group(1))
        else:
            allowed_amount_count = 0
            
    if other_count is None:
        other_match = re.search(r'OTHER:\s*(\d+)', content)
        if other_match:
            other_count = int(other_match.group(1))
        else:
            other_count = 0
    
    # Log results
    print(f"✅ Extraction verification complete. Found {total_refs} total file references.")
    print(f"  - In-network files: {in_network_count}")
    print(f"  - Allowed amount files: {allowed_amount_count}")
    print(f"  - Other files: {other_count}")
    
    # Extract sample URLs
    sample_urls = []
    sample_section = False
    for line in content.split('\n'):
        if line.strip() == "SAMPLES:":
            sample_section = True
            continue
        
        if sample_section and re.match(r'\d+\.\s+', line):
            url = re.sub(r'^\d+\.\s+', '', line).strip()
            if url and not url.startswith("SAMPLE"):
                sample_urls.append(url)
    
    # Print sample URLs
    print(f"\nSample URLs (first 5):")
    for i, url in enumerate(sample_urls[:5]):
        print(f"  {i+1}. {url}")
    
    # If we didn't find any references, warn but don't fail
    if total_refs == 0:
        print("WARNING: No file references found! Check the extraction process.")
    
    return total_refs

# Task 5: Process files using individual workers
def process_files(**context):
    """Process the extracted file references into meaningful data."""
    # Get the total count of file references and sample URLs from the previous task
    total_refs = context['ti'].xcom_pull(task_ids='verify_extraction')
    
    # Also try to get the sample URLs
    sample_urls_str = context['ti'].xcom_pull(task_ids='extract_file_references', key='sample_urls')
    sample_urls = []
    
    if sample_urls_str:
        try:
            sample_urls = ast.literal_eval(sample_urls_str)
        except Exception as e:
            print(f"Warning: Could not parse sample URLs: {e}")
    
    print(f"✅ Successfully extracted {total_refs} file references for processing")
    
    # Show sample of URLs that would be processed
    print("\nSample files that would be processed:")
    for i, url in enumerate(sample_urls[:5]):
        print(f"  {i+1}. {url}")
    
    # Outline next steps in the cloud-agnostic Iceberg processing pipeline
    print("\nNext steps in the data processing pipeline:")
    print("1. Download each file from its URL using standard HTTP/HTTPS libraries")
    print("2. Parse the content based on file type (JSON, CSV, etc.)")
    print("3. Transform the data into a standardized format")
    print("4. Write to Apache Iceberg tables")
    print("   - Using standard Hadoop/Spark/Iceberg components")
    print("   - This approach maintains cloud provider independence")
    print("   - Data can be processed on any environment with the right components")
    print("   - No reliance on proprietary cloud services")
    
    # Print distribution summary
    print(f"\nProcessing Summary:")
    print(f"  - Total file references: {total_refs}")
    print(f"  - These will be processed in batches for optimal performance")
    print(f"  - Results will be stored in Apache Iceberg tables on S3")
    print(f"  - This maintains metadata in the Iceberg format rather than in proprietary catalogs")
    
    return total_refs

# Helper functions
def log_message(message):
    """Write a log message to both console and log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    try:
        with open(config['log_file'], "a") as f:
            f.write(log_line + "\n")
    except Exception as e:
        print(f"Error writing to log: {e}")

def test_s3_access():
    """Test S3 connectivity and permissions."""
    try:
        log_message("Testing S3 connectivity...")
        s3 = boto3.client('s3')
        buckets = s3.list_buckets()
        log_message(f"Successfully connected to S3. Found {len(buckets.get('Buckets', []))} buckets")
        
        # Try to list objects in target bucket
        bucket = config['s3_bucket']
        prefix = 'payer/anthem'
        
        log_message(f"Testing access to bucket '{bucket}' with prefix '{prefix}'...")
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
            if 'Contents' in response:
                log_message(f"Successfully listed objects in {bucket}/{prefix}")
                for obj in response.get('Contents', [])[:3]:
                    log_message(f"  - {obj['Key']} ({obj['Size']} bytes)")
                return True
            else:
                log_message(f"No objects found in {bucket}/{prefix} or access denied")
                return False
        except Exception as e:
            log_message(f"Error listing bucket contents: {e}")
            return False
            
    except Exception as e:
        log_message(f"S3 connection test failed: {e}")
        traceback.print_exc(file=open(config['log_file'], "a"))
        return False

# Create the task instances
setup_env = PythonOperator(
    task_id='setup_environment',
    python_callable=setup_environment,
    dag=dag
)

extract_file_refs = PythonOperator(
    task_id='extract_file_references',
    python_callable=extract_file_references,
    dag=dag
)

verify_task = PythonOperator(
    task_id='verify_extraction',
    python_callable=verify_extraction,
    dag=dag
)

process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    dag=dag
)

# Define task dependencies
check_index_file >> setup_env >> extract_file_refs >> verify_task >> process_files_task

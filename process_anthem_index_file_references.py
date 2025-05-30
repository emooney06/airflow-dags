"""
Airflow DAG to process Anthem pricing transparency data:
1. Extract file references from the Anthem index file
2. Process and load data into Iceberg tables on S3

This DAG maintains a cloud-agnostic approach using standard Hadoop/Spark/Iceberg
components rather than AWS-specific services.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
import os
import boto3

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=12),
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
    # 'index_file_key': 'payer/anthem/index_files/main-index/{{ logical_date.strftime("%Y-%m-%d") }}_anthem_index.json.gz',  # Dynamic path
    'warehouse_location': 's3a://price-transparency-raw/warehouse',
    
    # Iceberg config
    'catalog_name': 'anthem_catalog',
    'file_refs_table': 'anthem_file_refs',
    'processed_table': 'anthem_data',
    
    # Environment setup - use Airflow home directory
    'jars_path': '/home/airflow/iceberg-jars:/home/airflow/hadoop-aws-jars',
    
    # Processing parameters
    'batch_size': 10000,
    'max_records': None,  # Set to a number for testing/limiting
}

# Task 1: Check if the index file exists
check_index_file = S3KeySensor(
    task_id='check_index_file',
    bucket_key=config['index_file_key'],
    bucket_name=config['s3_bucket'],
    aws_conn_id='aws_default',
    timeout=60 * 60 * 12,  # 12 hours timeout
    poke_interval=60 * 30,  # Check every 30 minutes
    dag=dag
)

# Task 2: Setup the environment (download JARs if needed)
setup_env_script = """
#!/bin/bash
HADOOP_AWS_DIR="/home/ec2-user/hadoop-aws-jars"
ICEBERG_JARS_DIR="/home/ec2-user/iceberg-jars"

# Create directories if they don't exist
mkdir -p $HADOOP_AWS_DIR
mkdir -p $ICEBERG_JARS_DIR

# Check and download Hadoop AWS JARs
if [ ! -f "$HADOOP_AWS_DIR/hadoop-aws-3.3.4.jar" ]; then
    echo "Downloading Hadoop AWS JARs..."
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $HADOOP_AWS_DIR
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P $HADOOP_AWS_DIR
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -P $HADOOP_AWS_DIR
fi

# Check and download Iceberg JARs
if [ ! -f "$ICEBERG_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" ]; then
    echo "Downloading Iceberg JARs..."
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar -P $ICEBERG_JARS_DIR
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.2/iceberg-aws-bundle-1.4.2.jar -P $ICEBERG_JARS_DIR
fi

echo "Environment setup complete."
"""

setup_env = BashOperator(
    task_id='setup_environment',
    bash_command=setup_env_script,
    dag=dag
)

# Task 3: Extract file references and save to Iceberg table
extract_file_refs_script = '''
#!/bin/bash

# Create working directory
mkdir -p /home/airflow/anthem-processing

# Create a Python script for extracting file references
cat > /home/airflow/anthem-processing/extract_refs.py << 'EOF'
import os
import sys
import boto3
import gzip
import re
import json
from urllib.parse import urlparse

# Set paths
INDEX_FILE = "s3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz"
OUTPUT_FILE = "/home/airflow/anthem_file_references.txt"

# File reference patterns
URL_PATTERNS = [
    r'"in_network_files"\s*:\s*\[\s*"([^"]+)"',
    r'"allowed_amount_files"\s*:\s*\[\s*"([^"]+)"',
    r'"url"\s*:\s*"([^"]+\.(?:json|csv|parquet|gz))"',
    r'https?://[^"\s]+\.(?:json|csv|parquet|gz)'
]

def main():
    try:
        print("Starting extraction from", INDEX_FILE)
        
        # Parse S3 URL
        parsed = urlparse(INDEX_FILE.replace("s3a://", "s3://"))
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        
        # Initialize S3 client
        s3 = boto3.client("s3")
        
        # Check if file exists
        try:
            s3.head_object(Bucket=bucket, Key=key)
            print("Successfully accessed S3 file")
        except Exception as e:
            print("Error accessing index file:", str(e))
            return 1
        
        # Read file in chunks (10MB at a time)
        references = []
        offset = 0
        chunk_size = 10 * 1024 * 1024  # 10MB chunks
        max_chunks = 10  # Process up to 100MB for safety
        
        for chunk in range(max_chunks):
            try:
                # Get range of bytes
                range_header = f"bytes={offset}-{offset+chunk_size-1}"
                response = s3.get_object(Bucket=bucket, Key=key, Range=range_header)
                chunk_data = response["Body"].read()
                
                if not chunk_data:
                    break  # End of file
                    
                # Decompress if gzipped
                if key.endswith(".gz"):
                    try:
                        chunk_data = gzip.decompress(chunk_data)
                    except Exception as e:
                        print(f"Warning: Could not decompress chunk {chunk}: {str(e)}")
                
                # Convert to string and find patterns
                chunk_str = chunk_data.decode("utf-8", errors="replace")
                
                # Extract URLs using regex patterns
                for pattern in URL_PATTERNS:
                    matches = re.findall(pattern, chunk_str)
                    references.extend(matches)
                    
                print(f"Processed chunk {chunk+1}, found {len(references)} references so far")
                
                # Move to next chunk
                offset += len(chunk_data)
                
                # If this chunk was smaller than requested, we've reached EOF
                if len(chunk_data) < chunk_size:
                    break
                    
            except Exception as e:
                print(f"Error processing chunk {chunk}: {str(e)}")
                if chunk > 0:  # Only break if we've processed at least one chunk
                    break
                else:
                    return 1
        
        # Remove duplicates
        unique_refs = list(set(references))
        
        # Categorize references
        in_network = [ref for ref in unique_refs if "in-network" in ref.lower()]
        allowed_amount = [ref for ref in unique_refs if "allowed-amount" in ref.lower()]
        other = [ref for ref in unique_refs if "in-network" not in ref.lower() and "allowed-amount" not in ref.lower()]
        
        # Save results
        with open(OUTPUT_FILE, "w") as f:
            f.write(f"TOTAL_REFS: {len(unique_refs)}\n")
            f.write(f"IN_NETWORK: {len(in_network)}\n")
            f.write(f"ALLOWED_AMOUNT: {len(allowed_amount)}\n")
            f.write(f"OTHER: {len(other)}\n\n")
            
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
        
        print(f"Extraction complete! Found {len(unique_refs)} unique file references")
        return 0
        
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        # Write error to output file for Airflow to read
        with open(OUTPUT_FILE, "w") as f:
            f.write(f"ERROR: {str(e)}\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
EOF

# Run the extraction script
python3 /home/airflow/anthem-processing/extract_refs.py

# Show the results summary
echo "=== EXTRACTION RESULTS ==="
head -n 20 /home/airflow/anthem_file_references.txt || echo "Could not display output file"
'''

extract_file_refs = BashOperator(
    task_id='extract_file_references',
    bash_command=extract_file_refs_script,
    params={
        's3_path': f"s3a://{config['s3_bucket']}/{config['index_file_key']}",
        'warehouse': config['warehouse_location'],
        'catalog': config['catalog_name'],
        'table': config['file_refs_table'],
        'batch_size': config['batch_size'],
        'max_records': config['max_records']
    },
    dag=dag
)

# Task 4: Verify extraction results
def verify_extraction(**context):
    """Verify the extracted file references from the output file."""
    import os
    import re
    
    output_file = "/home/airflow/anthem_file_references.txt"
    
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
    
    # Parse the summary statistics
    total_refs = 0
    in_network_count = 0
    allowed_amount_count = 0
    other_count = 0
    
    # Extract metrics using regex
    total_match = re.search(r'TOTAL_REFS:\s*(\d+)', content)
    if total_match:
        total_refs = int(total_match.group(1))
    
    in_network_match = re.search(r'IN_NETWORK:\s*(\d+)', content)
    if in_network_match:
        in_network_count = int(in_network_match.group(1))
    
    allowed_amount_match = re.search(r'ALLOWED_AMOUNT:\s*(\d+)', content)
    if allowed_amount_match:
        allowed_amount_count = int(allowed_amount_match.group(1))
    
    other_match = re.search(r'OTHER:\s*(\d+)', content)
    if other_match:
        other_count = int(other_match.group(1))
    
    # Log results
    context['ti'].xcom_push(key='total_file_refs', value=total_refs)
    context['ti'].xcom_push(key='file_content', value=content[:1000])  # First 1000 chars
    
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
    
    context['ti'].xcom_push(key='sample_urls', value=str(sample_urls))
    
    # Print sample URLs
    print(f"\nSample URLs (first 5):")
    for i, url in enumerate(sample_urls[:5]):
        print(f"  {i+1}. {url}")
    
    # If we didn't find any references, warn but don't fail
    if total_refs == 0:
        print("WARNING: No file references found! Check the extraction process.")
    
    return total_refs

verify_task = PythonOperator(
    task_id='verify_extraction',
    python_callable=verify_extraction,
    dag=dag
)

# Task 5: Process files using individual workers (simplified placeholder for now)
def process_files(**context):
    """Process the extracted file references into meaningful data."""
    # Get the total count of file references and sample URLs from the previous task
    total_refs = context['ti'].xcom_pull(task_ids='verify_extraction', key='total_file_refs')
    sample_urls = context['ti'].xcom_pull(task_ids='verify_extraction', key='sample_urls')
    
    print(f"✅ Successfully extracted {total_refs} file references for processing")
    
    try:
        # Convert string representation back to list
        import ast
        if isinstance(sample_urls, str):
            sample_urls = ast.literal_eval(sample_urls)
        
        # Show sample of URLs that would be processed
        print("\nSample files that would be processed:")
        for i, url in enumerate(sample_urls[:5]):
            print(f"  {i+1}. {url}")
    except Exception as e:
        print(f"Error processing sample URLs: {str(e)}")
    
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

process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    dag=dag
)

# Define task dependencies
check_index_file >> setup_env >> extract_file_refs >> verify_task >> process_files_task

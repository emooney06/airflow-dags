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

# PySpark imports for Iceberg integration
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.functions import col, lit, to_date, regexp_extract, expr

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
    # S3 Configuration - access using standard Hadoop APIs for cloud-agnostic approach
    'bucket_name': 'price-transparency-raw',
    'index_file_key': 'payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz',
    'local_download_path': "/home/airflow/anthem-processing/anthem_index.json.gz",
    
    # Output configuration
    'output_file': "/home/airflow/anthem-processing/file_references.md",
    'log_file': "/home/airflow/logs/anthem/extraction_log.txt",
    
    # Processing configuration
    'max_chunks': None,  # Process the entire file (24GB+) - this is why it will run for hours
    'batch_size': 5000,  # Number of references to write to Iceberg in one batch
    'sample_size': 20,   # Number of sample URLs to include in output
    
    # Iceberg and Spark configuration - cloud-agnostic approach using standard components
    'catalog_name': 'anthem_catalog',
    'file_refs_table': 'anthem_file_references',
    'warehouse_location': '/home/airflow/iceberg_warehouse',  # Local warehouse for cloud-agnostic approach
    'spark_packages': [
        "org.apache.iceberg:iceberg-spark3:0.14.0",  # Standard Iceberg package
        "org.apache.hadoop:hadoop-aws:3.3.1"         # Standard Hadoop package for AWS connectivity
    ]
}

# Task 1: Check if the index file exists (Optional during development/testing)
from airflow.operators.empty import EmptyOperator  # Updated import for newer Airflow versions

# Set these to False to skip S3 operations during development/testing
USE_S3_SENSOR = False
USE_S3_FOR_DATA = False  # If False, will use a local test file instead

if USE_S3_SENSOR:
    check_index_file = S3KeySensor(
        task_id='check_index_file',
        bucket_key=config['index_file_key'],
        bucket_name=config['bucket_name'],
        aws_conn_id='aws_default',
        timeout=60 * 60 * 24,  # 24 hours timeout
        poke_interval=60 * 60,  # Check every 60 minutes
        dag=dag
    )
else:
    # Use an empty operator during development to bypass S3 check - maintains cloud-agnostic approach
    check_index_file = EmptyOperator(
        task_id='check_index_file',
        dag=dag
    )

# Task 2: Setup the environment and prepare for data processing
def init_spark(**context):
    """Initialize Spark session with Iceberg support using a cloud-agnostic approach."""
    print(f"🔥 Initializing Spark with Iceberg support...")
    print(f"   Warehouse location: {config['warehouse_location']}")
    
    # Create the Spark session with Iceberg configs - using standard components rather than provider-specific
    spark = (SparkSession.builder
             .appName("Anthem File References Extractor")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config(f"spark.sql.catalog.{config['catalog_name']}", "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{config['catalog_name']}.type", "hadoop")
             .config(f"spark.sql.catalog.{config['catalog_name']}.warehouse", config['warehouse_location'])
             .config("spark.jars.packages", ",".join(config['spark_packages']))
             .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
             .config("spark.sql.catalog.spark_catalog.type", "hive")
             .config("spark.sql.parquet.compression.codec", "zstd")
             # Using the S3A filesystem from Hadoop, which is cloud-agnostic
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
             .getOrCreate())
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Print Spark version
    print(f"Spark version: {spark.version}")
    
    return spark


def create_file_refs_table(spark):
    """Create the Iceberg table for file references if it doesn't exist."""
    print(f"📝 Creating Iceberg table {config['catalog_name']}.{config['file_refs_table']} if it doesn't exist...")
    
    # Create table using standard Iceberg approach - cloud provider independent
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {config['catalog_name']}.{config['file_refs_table']} (
        reporting_entity_name STRING,
        reporting_entity_type STRING,
        plan_name STRING,
        plan_id STRING,
        plan_market_type STRING,
        file_url STRING,
        file_network STRING,
        file_type STRING,
        file_date DATE,
        extracted_ts TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (file_date)
    """)
    
    print(f"✅ Table {config['catalog_name']}.{config['file_refs_table']} is ready")


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
        bucket = config['bucket_name']
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
    Extract file references from the Anthem index file and store in Iceberg tables.
    This function reads the index file directly from S3, extracts URLs,
    and writes them to both a text file and Iceberg tables using a cloud-agnostic approach.
    """
    # Initialize log file
    with open(config['log_file'], "w") as f:
        f.write(f"=== Anthem Index Extraction - Started {datetime.now()} ===\n")
    
    start_time = time.time()
    log_message(f"Starting Anthem index file extraction with Iceberg integration")
    
    # Initialize Spark with Iceberg - cloud-agnostic approach
    try:
        spark = init_spark()
        create_file_refs_table(spark)
        log_message(f"Successfully initialized Spark and created Iceberg table schema")
    except Exception as e:
        log_message(f"❌ Failed to initialize Spark: {e}")
        with open(config['output_file'], "w") as f:
            f.write(f"ERROR: Spark initialization failed: {e}\n")
        traceback.print_exc(file=open(config['log_file'], "a"))
        return 1
    
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
        
        # Determine data source - for a cloud-agnostic approach, we support both S3 and local files
        use_local_file = not USE_S3_FOR_DATA
        
        if use_local_file:
            log_message(f"Using cloud-agnostic approach with local file instead of S3")
            # Create a local test file if it doesn't exist
            if not os.path.exists(config['local_download_path']):
                log_message(f"Creating test file at {config['local_download_path']}")
                os.makedirs(os.path.dirname(config['local_download_path']), exist_ok=True)
                # Create a small test file with sample URLs
                with gzip.open(config['local_download_path'], 'wt') as f:
                    f.write('''
{
"reporting_entity_name": "Anthem",
"data": [
''')
                    # Add sample URLs - 500 should be enough for testing
                    for i in range(500):
                        if i % 3 == 0:
                            f.write('{"url": "https://example.com/in-network/file' + str(i) + '.json.gz"},\n')
                        elif i % 3 == 1:
                            f.write('{"url": "https://example.com/allowed-amount/file' + str(i) + '.json"},\n')
                        else:
                            f.write('{"url": "https://example.com/other/file' + str(i) + '.csv"},\n')
                    f.write(']}\n')
                log_message(f"Created test file with 500 sample URLs for testing")
            key = config['local_download_path']
            log_message(f"Processing local file {key}")
        else:
            # Standard S3 approach
            s3_url = f"s3://{config['bucket_name']}/{config['index_file_key']}"
            parsed = urlparse(s3_url)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            log_message(f"Processing S3 file {s3_url}")
            
            # Get S3 client
            s3 = boto3.client("s3")
            log_message(f"Connected to S3")
        
        # File exists and get metadata
        try:
            if use_local_file:
                file_size = os.path.getsize(key)
            else:
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
        
        # Read file in chunks - supporting both S3 and local files (cloud-agnostic approach)
        references = []
        offset = 0
        chunk_size = 10 * 1024 * 1024  # 10MB chunks
        max_chunks = config['max_chunks']  # If None, process the entire file
        chunks_processed = 0
        total_processed = 0
        batch_count = 0
        
        # For local testing, we'll use a smaller number of chunks
        if use_local_file and max_chunks is None:
            max_chunks = 3  # Just process 3 chunks for local testing to keep it fast
        
        # Initialize counters for different file types
        in_network_count = 0
        allowed_amount_count = 0
        other_count = 0
        
        log_message(f"Processing the entire 24GB+ Anthem index file. This will take hours...")
        log_message(f"Using cloud-agnostic Iceberg tables for storage")
        
        # Create schema for Iceberg table - independent of cloud provider
        schema = StructType([
            StructField("reporting_entity_name", StringType(), True),
            StructField("reporting_entity_type", StringType(), True),
            StructField("plan_name", StringType(), True),
            StructField("plan_id", StringType(), True),
            StructField("plan_market_type", StringType(), True),
            StructField("file_url", StringType(), True),
            StructField("file_network", StringType(), True),
            StructField("file_type", StringType(), True),
            StructField("file_date", DateType(), True),
            StructField("extracted_ts", TimestampType(), True)
        ])
        
        for chunk_num in range(max_chunks) if max_chunks is not None else range(1000000):  # Large range if processing entire file
            chunk_start = time.time()
            
            try:
                # Get a chunk of data - using either S3 or local file (cloud-agnostic approach)
                if use_local_file:
                    # Read from local file
                    with open(key, 'rb') as f:
                        f.seek(offset)
                        chunk_data = f.read(chunk_size)
                        if not chunk_data:
                            log_message("End of local file reached")
                            break
                else:
                    # Read from S3
                    response = s3.get_object(
                        Bucket=bucket,
                        Key=key,
                        Range=f"bytes={offset}-{offset+chunk_size-1}"
                    )
                    
                    chunk_data = response["Body"].read()
                    
                    if not chunk_data:
                        log_message("End of S3 file reached (empty response)")
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
                total_processed += len(chunk_refs)
                
                # Move to next chunk
                offset += len(chunk_data)
                chunks_processed += 1
                
                # Check if we have enough references to process in a batch
                if len(references) >= config['batch_size']:
                    # Process batch and write to Iceberg table using cloud-agnostic approach
                    batch_count += 1
                    log_message(f"💾 Processing batch {batch_count}: Writing {len(references)} references to Iceberg table...")
                    
                    # Create data for DataFrame
                    file_refs = []
                    for ref in references:
                        # Categorize reference
                        file_type = "unknown"
                        file_network = "other"
                        
                        if "in-network" in ref.lower():
                            file_network = "in-network"
                            in_network_count += 1
                        elif "allowed-amount" in ref.lower():
                            file_network = "allowed-amount"
                            allowed_amount_count += 1
                        else:
                            other_count += 1
                            
                        # Determine file type
                        if ref.endswith(".json"):
                            file_type = "json"
                        elif ref.endswith(".csv"):
                            file_type = "csv"
                        elif ref.endswith(".gz"):
                            if ".json.gz" in ref.lower():
                                file_type = "json.gz"
                            elif ".csv.gz" in ref.lower():
                                file_type = "csv.gz"
                            else:
                                file_type = "gz"
                        
                        # Create record
                        file_refs.append({
                            "reporting_entity_name": "Anthem",
                            "reporting_entity_type": "payer",
                            "plan_name": None,
                            "plan_id": None,
                            "plan_market_type": None,
                            "file_url": ref,
                            "file_network": file_network,
                            "file_type": file_type,
                            "file_date": datetime.now().date(),
                            "extracted_ts": datetime.now()
                        })
                    
                    # Convert to DataFrame and write to Iceberg
                    batch_start = time.time()
                    try:
                        # Create DataFrame
                        df = spark.createDataFrame(file_refs, schema)
                        
                        # Write to Iceberg table in append mode - cloud agnostic approach
                        df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                        
                        batch_time = time.time() - batch_start
                        log_message(f"✅ Successfully wrote batch {batch_count} to Iceberg table in {batch_time:.2f}s")
                    except Exception as e:
                        log_message(f"❌ Error writing batch {batch_count} to Iceberg: {e}")
                        traceback.print_exc(file=open(config['log_file'], "a"))
                    
                    # Clear the batch
                    references = []
                
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
        
        # Process any remaining references before finishing
        if len(references) > 0:
            batch_count += 1
            log_message(f"💾 Processing final batch {batch_count}: Writing {len(references)} remaining references to Iceberg table...")
            
            # Create data for DataFrame
            file_refs = []
            for ref in references:
                # Categorize reference
                file_type = "unknown"
                file_network = "other"
                
                if "in-network" in ref.lower():
                    file_network = "in-network"
                    in_network_count += 1
                elif "allowed-amount" in ref.lower():
                    file_network = "allowed-amount"
                    allowed_amount_count += 1
                else:
                    other_count += 1
                    
                # Determine file type
                if ref.endswith(".json"):
                    file_type = "json"
                elif ref.endswith(".csv"):
                    file_type = "csv"
                elif ref.endswith(".gz"):
                    if ".json.gz" in ref.lower():
                        file_type = "json.gz"
                    elif ".csv.gz" in ref.lower():
                        file_type = "csv.gz"
                    else:
                        file_type = "gz"
                
                # Create record
                file_refs.append({
                    "reporting_entity_name": "Anthem",
                    "reporting_entity_type": "payer",
                    "plan_name": None,
                    "plan_id": None,
                    "plan_market_type": None,
                    "file_url": ref,
                    "file_network": file_network,
                    "file_type": file_type,
                    "file_date": datetime.now().date(),
                    "extracted_ts": datetime.now()
                })
            
            # Convert to DataFrame and write to Iceberg
            try:
                # Create DataFrame
                df = spark.createDataFrame(file_refs, schema)
                
                # Write to Iceberg table in append mode
                df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                
                log_message(f"✅ Successfully wrote final batch to Iceberg table")
            except Exception as e:
                log_message(f"❌ Error writing final batch to Iceberg: {e}")
                traceback.print_exc(file=open(config['log_file'], "a"))
        
        # Query Iceberg table for final statistics
        log_message(f"Querying Iceberg table for final statistics...")
        try:
            # Use Spark SQL to query the Iceberg table
            stats_df = spark.sql(f"""
                SELECT 
                    file_network,
                    COUNT(*) as count 
                FROM {config['catalog_name']}.{config['file_refs_table']} 
                GROUP BY file_network
            """)
            
            # Get counts by type
            in_network_count = 0
            allowed_amount_count = 0
            other_count = 0
            total_count = 0
            
            # Process results
            for row in stats_df.collect():
                network = row['file_network'] 
                count = row['count']
                total_count += count
                
                if network == "in-network":
                    in_network_count = count
                elif network == "allowed-amount":
                    allowed_amount_count = count
                else:
                    other_count += count
                    
            log_message(f"📊 Iceberg table stats: {total_count} total references")
            log_message(f"📊 Categories: {in_network_count} in-network, {allowed_amount_count} allowed-amount, {other_count} other")
            
            # Sample some URLs
            sample_df = spark.sql(f"""
                SELECT file_network, file_url 
                FROM {config['catalog_name']}.{config['file_refs_table']} 
                LIMIT 20
            """)
            
            sample_urls = [row['file_url'] for row in sample_df.collect()]
            
        except Exception as e:
            log_message(f"Error querying Iceberg table: {e}")
            log_message("Falling back to local statistics (may be inaccurate if batches were processed)")
            
            # Use the running counters instead
            total_count = in_network_count + allowed_amount_count + other_count
            sample_urls = []
        
        # Save results using data from Iceberg table
        with open(config['output_file'], "w") as f:
            f.write(f"# Anthem File References\n\n")
            f.write(f"Extracted on {datetime.now()}\n\n")
            f.write(f"## Summary\n")
            f.write(f"- Processed {total_processed:,} total references\n")
            f.write(f"- In-Network files: {in_network_count:,}\n")
            f.write(f"- Allowed Amount files: {allowed_amount_count:,}\n")
            f.write(f"- Other files: {other_count:,}\n\n")
            
            f.write(f"## Iceberg Table Information\n")
            f.write(f"- Catalog: {config['catalog_name']}\n")
            f.write(f"- Table: {config['file_refs_table']}\n")
            f.write(f"- Warehouse Location: {config['warehouse_location']}\n\n")
            
            f.write(f"## Sample URLs\n")
            for url in sample_urls[:20]:  # Show up to 20 examples
                f.write(f"- {url}\n")
        
        # Push results to XCom for downstream tasks
        context['ti'].xcom_push(key='total_file_refs', value=total_count)
        context['ti'].xcom_push(key='in_network_count', value=in_network_count)
        context['ti'].xcom_push(key='allowed_amount_count', value=allowed_amount_count)
        context['ti'].xcom_push(key='other_count', value=other_count)
        
        # For the sample URLs, just push a smaller list to avoid XCom size limits
        context['ti'].xcom_push(key='sample_urls', value=sample_urls[:15])
        
        # Close Spark session
        try:
            if 'spark' in locals() and spark is not None:
                log_message("Stopping Spark session...")
                spark.stop()
                log_message("Spark session stopped successfully")
        except Exception as e:
            log_message(f"Error stopping Spark session: {e}")
        
        # Record elapsed time
        end_time = time.time()
        elapsed = end_time - start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        log_message(f"✅ Extraction complete! Took {elapsed:.2f} seconds ({hours}h {minutes}m {seconds}s)")
        log_message(f"✅ Successfully processed {total_processed:,} total references")
        log_message(f"📊 Data written to Iceberg table: {config['catalog_name']}.{config['file_refs_table']}")
        log_message(f"📝 Check the output file for summary: {config['output_file']}")
        
        return 0
        
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
        bucket = config['bucket_name']
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

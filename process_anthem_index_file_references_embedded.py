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
import subprocess
import logging
import requests
from urllib.parse import urlparse

# PySpark imports for Iceberg integration
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.functions import col, lit, to_date, regexp_extract, expr

# from airflow import DAG # Replaced by TaskFlow
# from airflow.operators.python import PythonOperator # Replaced by TaskFlow
from airflow.decorators import dag, task # Added for TaskFlow API
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# Feature flags for the cloud-agnostic approach
# These can be toggled to enable/disable specific features during development and testing
USE_S3_SENSOR = False      # False = Use EmptyOperator instead of S3 sensor (more portable)
USE_S3_FOR_DATA = True    # False = Use local files instead of S3 (cloud-agnostic)
USE_SPARK_ICEBERG = True   # True = Use Iceberg for data lake (core technology)

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

# DAG definition will be done using @dag decorator later in the file

# Configuration - Exact parameters from shell script command
config = {
    # Exact S3 paths from shell script command
    'index_file': 's3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz',
    'warehouse_location': 's3a://price-transparency-raw/warehouse',
    'catalog_name': 'anthem_catalog',
    'file_refs_table': 'anthem_file_refs',
    
    # Output configuration - use airflow home directory
    'output_dir': os.path.expanduser("~/anthem-processing"),
    'output_file': os.path.expanduser("~/anthem-processing/file_references.txt"),
    'log_file': os.path.expanduser("~/anthem-processing/extraction_log.txt"),
    'summary_file': os.path.expanduser("~/anthem-processing/summary.txt"),
    
    # Processing configuration
    'max_chunks': None,     # Process the entire file - will take hours
    'batch_size': 10000,    # Batch size for Iceberg writes
    
    # S3 parameters for Airflow operators
    'bucket_name': 'price-transparency-raw', 
    'index_file_key': 'payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz',
    
    # JAR files configuration - using user's home directory for portability
    'jar_dir': os.path.expanduser('~/jars'),
    'jar_files': {
        'iceberg-spark': {
            'path': os.path.join(os.path.expanduser('~/jars'), 'iceberg-spark-runtime-3.3_2.12-1.4.2.jar'), # Switched to Spark 3.3 version for Java 11 compatibility
            'url': 'https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar' # Updated URL
        },
        'hadoop-aws': {
            'path': os.path.join(os.path.expanduser('~/jars'), 'hadoop-aws-3.3.4.jar'),
            'url': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar'
        },
        'aws-java-sdk': {
            'path': os.path.join(os.path.expanduser('~/jars'), 'aws-java-sdk-bundle-1.12.262.jar'),
            'url': 'https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar'
        }
    }
}

# Task 2: Setup the environment and prepare for data processing
def init_spark(**context):
    """Initialize Spark session with Iceberg support exactly like the shell script.
    This implementation uses a cloud-agnostic approach that works with any storage system.
    """
    import logging
    import os
    
    logger = logging.getLogger('airflow.task')
    logger.info(f"Initializing Spark with Iceberg support...")
    logger.info(f"Warehouse location: {config['warehouse_location']}")
    
    # Create JAR directory if it doesn't exist
    os.makedirs(config['jar_dir'], exist_ok=True)
    
    # Check if jars exist and download if needed
    jar_paths = []
    for jar_name, jar_info in config['jar_files'].items():
        jar_path = jar_info['path']
        jar_paths.append(jar_path)
        
        # Download JAR if it doesn't exist
        if not os.path.exists(jar_path):
            jar_url = jar_info['url']
            logger.info(f"Downloading {jar_name} JAR from {jar_url}")
            try:
                # Create parent directory if it doesn't exist
                os.makedirs(os.path.dirname(jar_path), exist_ok=True)
                
                # Download the JAR file
                import requests
                response = requests.get(jar_url, stream=True)
                response.raise_for_status()
                
                with open(jar_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Successfully downloaded {jar_path}")
            except Exception as e:
                error_msg = f"Failed to download {jar_name} JAR: {str(e)}"
                logger.error(error_msg)
                with open(config['log_file'], 'a') as f:
                    f.write(f"ERROR: {error_msg}\n")
        else:
            logger.info(f"JAR file already exists: {jar_path}")
    
    # Make sure environment variables are set just like in the shell script
    jars_path = ",".join(jar_paths)
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {jars_path} pyspark-shell'
    logger.info(f"Set PYSPARK_SUBMIT_ARGS: {os.environ['PYSPARK_SUBMIT_ARGS']}")
    
    # If we're not using S3, update the warehouse location to a local path
    warehouse_location = config['warehouse_location']
    if not USE_S3_FOR_DATA and warehouse_location.startswith('s3'):
        warehouse_location = f"file://{config['output_dir']}/warehouse"
        logger.info(f"Using local warehouse location: {warehouse_location}")
    else:
        logger.info(f"Using S3 warehouse location: {warehouse_location}")
    
    # Create the Spark session with cloud-agnostic configuration
    builder = (SparkSession.builder
             .appName("Anthem File References Extractor")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config(f"spark.sql.catalog.{config['catalog_name']}", "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{config['catalog_name']}.type", "hadoop")
             .config(f"spark.sql.catalog.{config['catalog_name']}.warehouse", warehouse_location)
             .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
             .config("spark.sql.catalog.spark_catalog.type", "hive")
             .config("spark.sql.parquet.compression.codec", "zstd")
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
    )
    
    # Add S3-specific configs only if using S3
    if USE_S3_FOR_DATA:
        builder = (builder
            .config(f"spark.sql.catalog.{config['catalog_name']}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{config['catalog_name']}.default-namespace", "default")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.path.style.access", "true"))
    
    # Create the Spark session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Log Spark version
    logger.info(f"Spark version: {spark.version}")
    
    return spark


def create_file_refs_table(spark):
    """Create the Iceberg table for file references if it doesn't exist.
    Uses exactly the same table structure as the shell script.
    """
    import logging
    logger = logging.getLogger('airflow.task')
    
    logger.info(f"Inside create_file_refs_table. Configured catalog_name: '{config.get('catalog_name')}', file_refs_table: '{config.get('file_refs_table')}'")
    logger.info(f"Creating Iceberg table {config['catalog_name']}.{config['file_refs_table']} if it doesn't exist...")
    
    # Create table with exact same schema as the shell script
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
    
    # Check if table was created successfully
    tables = spark.sql(f"SHOW TABLES IN {config['catalog_name']}").collect()
    table_exists = False
    for table in tables:
        if table.tableName == config['file_refs_table']:
            table_exists = True
            break
    
    if table_exists:
        logger.info(f"Table {config['catalog_name']}.{config['file_refs_table']} is ready")
    else:
        error_msg = f"Failed to create table {config['catalog_name']}.{config['file_refs_table']}"
        logger.error(error_msg)
        with open(config['log_file'], 'a') as f:
            f.write(f"ERROR: {error_msg}\n")
        raise RuntimeError(error_msg)
    
    return True


@task
def setup_environment_task():
    """Create working directory and prepare the environment.
    Download required JARs for Spark and Iceberg exactly like the shell script.
    """
    import subprocess
    import os
    import requests
    import logging
    
    # Set up a logger
    logger = logging.getLogger('airflow.task')
    
    # Create necessary directories
    logger.info("Creating required directories...")
    os.makedirs(config['output_dir'], exist_ok=True)
    os.makedirs(config['jar_dir'], exist_ok=True)
    
    # Prepare log file
    with open(config['log_file'], 'w') as f:
        f.write(f"=== Anthem Index File Processing - Started {datetime.now()} ===\n")
    
    # Download JAR files if needed
    logger.info("Checking for required JAR files...")
    jar_paths = []
    
    for jar_name, jar_info in config['jar_files'].items():
        jar_path = jar_info['path']
        jar_url = jar_info['url']
        jar_paths.append(jar_path)
        
        if not os.path.exists(jar_path):
            logger.info(f"Downloading {jar_name} JAR from {jar_url}")
            try:
                # Create parent directory if it doesn't exist
                os.makedirs(os.path.dirname(jar_path), exist_ok=True)
                
                # Download the JAR file
                response = requests.get(jar_url, stream=True)
                response.raise_for_status()
                
                with open(jar_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Successfully downloaded {jar_path}")
            except Exception as e:
                error_msg = f"Failed to download {jar_name} JAR: {str(e)}"
                logger.error(error_msg)
                with open(config['log_file'], 'a') as f:
                    f.write(f"ERROR: {error_msg}\n")
                raise RuntimeError(error_msg)
        else:
            logger.info(f"JAR file already exists: {jar_path}")
    
    # Set environment variables for PySpark - exactly like the shell script
    jar_paths_str = ','.join(jar_paths)
    os.environ['PYSPARK_SUBMIT_ARGS'] = f"--jars {jar_paths_str} pyspark-shell"
    logger.info(f"Set PYSPARK_SUBMIT_ARGS to: {os.environ['PYSPARK_SUBMIT_ARGS']}")
    
    # jar_paths will be returned by this task for XComs.
    
    # Test S3 connectivity - only if we're using S3
    if USE_S3_FOR_DATA:
        logger.info("Testing S3 connectivity...")
        try:
            s3 = boto3.client('s3')
            s3.list_buckets()  # Just to test connectivity
            
            # Check if we can access the index file
            parsed_url = urlparse(config['index_file'])
            bucket = parsed_url.netloc
            key = parsed_url.path.lstrip('/')
            
            try:
                s3.head_object(Bucket=bucket, Key=key)
                logger.info(f"Successfully verified access to index file: {config['index_file']}")
            except Exception as e:
                error_msg = f"Cannot access index file: {str(e)}"
                logger.warning(error_msg)
                logger.warning("Will try to continue with local files since USE_S3_FOR_DATA is enabled.")
                with open(config['log_file'], 'a') as f:
                    f.write(f"WARNING: {error_msg}\n")
                    f.write("Will try to continue with local files.\n")
        except Exception as e:
            error_msg = f"S3 connectivity test failed: {str(e)}"
            logger.warning(error_msg)
            logger.warning("Will try to continue with local files since USE_S3_FOR_DATA is enabled.")
            with open(config['log_file'], 'a') as f:
                f.write(f"WARNING: {error_msg}\n")
                f.write("Will try to continue with local files.\n")
    else:
        logger.info("Skipping S3 connectivity test since USE_S3_FOR_DATA is disabled.")
        logger.info("Using cloud-agnostic approach with local files.")
        with open(config['log_file'], 'a') as f:
            f.write("Skipping S3 connectivity test, using local files for cloud-agnostic approach.\n")
    
    logger.info("Environment setup completed successfully")
    return jar_paths

# Task 3: Extract file references and save to output file
@task
def extract_file_references_task(setup_output=None): # Accepts output from setup_task if needed
    """
    Extract file references from the Anthem index file and store in Iceberg tables.
    This function implements a cloud-agnostic approach using standard components rather
    than AWS-specific services, making it portable across different environments.
    """
    import logging
    import os
    from urllib.parse import urlparse
    import time
    
    logger = logging.getLogger('airflow.task')
    
    # Initialize log file if it doesn't already exist
    if not os.path.exists(config['log_file']):
        with open(config['log_file'], "w") as f:
            f.write(f"=== Anthem Index Extraction - Started {datetime.now()} ===\n")
    else:
        with open(config['log_file'], "a") as f:
            f.write(f"\n=== Starting file reference extraction {datetime.now()} ===\n")
    
    start_time = time.time()
    logger.info(f"Starting Anthem index file extraction with Iceberg integration")
    
    # Patterns for extracting URLs from the index file
    url_patterns = [
        r'https?://[^\s"\'\)\(\[\]\{\}]+',  # Simple URL pattern
        r'"url"\s*:\s*"(https?://[^"]+)"'     # JSON URL pattern with capture group
    ]
    
    try:
        # Initialize Spark with Iceberg using cloud-agnostic configuration
        logger.info(f"Initializing Spark with Iceberg integration")
        # Create output directories if they don't exist
        os.makedirs(config['output_dir'], exist_ok=True)
        os.makedirs(os.path.dirname(config['output_file']), exist_ok=True)
        os.makedirs(os.path.dirname(config['log_file']), exist_ok=True)
        os.makedirs(os.path.dirname(config['summary_file']), exist_ok=True)
        
        # For cloud-agnostic operation, let's adjust the index file path if needed
        if not USE_S3_FOR_DATA and config['index_file'].startswith('s3'):
            # Create a local test file for processing
            local_file_path = f"{config['output_dir']}/anthem_index_test.json.gz"
            logger.info(f"Using local file for testing: {local_file_path}")
            
            # Generate a small test file if it doesn't exist
            if not os.path.exists(local_file_path):
                logger.info("Creating local test file with sample data")
                try:
                    import gzip
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    with gzip.open(local_file_path, 'wt') as f:
                        f.write('{"reporting_entity_name": "Anthem", "reporting_entity_type": "payer", "data": [\n')
                        # Add 100 sample URLs
                        for i in range(100):
                            if i % 3 == 0:
                                f.write('{"url": "https://example.com/in-network/file' + str(i) + '.json.gz"},\n')
                            elif i % 3 == 1:
                                f.write('{"url": "https://example.com/allowed-amount/file' + str(i) + '.json"},\n')
                            else:
                                f.write('{"url": "https://example.com/other/file' + str(i) + '.csv"},\n')
                        f.write(']}\n')
                    logger.info(f"Created local test file: {local_file_path}")
                except Exception as e:
                    logger.warning(f"Error creating local test file: {e}")
            
            # Use the local file instead of S3
            config['index_file'] = f"file://{local_file_path}"
            logger.info(f"Using local file path: {config['index_file']}")
        
        # Initialize Spark with JAR files for Iceberg
        spark = init_spark()
        
        # Create the Iceberg table if it doesn't exist - using standard Iceberg API
        logger.info(f"Creating Iceberg table {config['catalog_name']}.{config['file_refs_table']} if it doesn't exist")
        create_file_refs_table(spark)
        logger.info(f"Successfully initialized Spark and created Iceberg table schema")
    except Exception as e:
        error_msg = f"Failed to initialize Spark: {e}"
        logger.error(error_msg)
        with open(config['output_file'], "w") as f:
            f.write(f"ERROR: {error_msg}\n")
            traceback.print_exc(file=f)
        raise RuntimeError(error_msg)
    
    try:
        # Define the schema for the Iceberg table
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
        
        logger.info(f"Processing index file: {config['index_file']}")
        
        # Initialize counters and lists
        total_records_written_to_iceberg = 0
        in_network_count = 0
        allowed_amount_count = 0
        other_count = 0
        batch_count = 0
        
        all_extracted_records_for_batching = [] # Stores dicts for DataFrame creation
        all_extracted_urls = [] # Stores all raw URLs for sampling

        logger.info(f"Reading index file using Spark: {config['index_file']}")
        df_index = spark.read.text(config['index_file'])
        
        logger.info(f"Processing index file content. This may take a while...")

        for row in df_index.collect(): # df.collect() can be memory intensive for very large files.
                                      # Consider df.rdd.toLocalIterator() or df.toLocalIterator() for Spark 3.4+
                                      # if memory becomes an issue and iterative processing is preferred.
            line = row[0]
            for pattern in url_patterns: # url_patterns is a global variable
                matches = re.findall(pattern, line)
                for match in matches:
                    url = match[0] if isinstance(match, tuple) else match
                    if not url.startswith('http'):
                        continue

                    all_extracted_urls.append(url)

                    # Categorize reference
                    file_network = "other"
                    if "in-network" in url.lower():
                        file_network = "in-network"
                        in_network_count += 1
                    elif "allowed-amount" in url.lower():
                        file_network = "allowed-amount"
                        allowed_amount_count += 1
                    else:
                        other_count += 1
                    
                    # Determine file type more specifically
                    file_type = "unknown"
                    if url.endswith(".json.gz"):
                        file_type = "json.gz"
                    elif url.endswith(".csv.gz"):
                        file_type = "csv.gz"
                    elif url.endswith(".json"):
                        file_type = "json"
                    elif url.endswith(".csv"):
                        file_type = "csv"
                    elif url.endswith(".gz"): # generic gz if not more specific
                        file_type = "gz"
                    
                    record = {
                        "reporting_entity_name": "Anthem",
                        "reporting_entity_type": "payer",
                        "plan_name": None, # Placeholder, to be enriched later if possible
                        "plan_id": None, # Placeholder
                        "plan_market_type": None, # Placeholder
                        "file_url": url,
                        "file_network": file_network,
                        "file_type": file_type,
                        "file_date": datetime.now().date(), # Or parse from URL/index if available
                        "extracted_ts": datetime.now()
                    }
                    all_extracted_records_for_batching.append(record)

                    if len(all_extracted_records_for_batching) >= config['batch_size']:
                        batch_count += 1
                        logger.info(f"Writing batch {batch_count} with {len(all_extracted_records_for_batching)} references to Iceberg table.")
                        try:
                            batch_df = spark.createDataFrame(all_extracted_records_for_batching, schema)
                            batch_df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                            total_records_written_to_iceberg += len(all_extracted_records_for_batching)
                            logger.info(f"Successfully wrote batch {batch_count}. Total records written so far: {total_records_written_to_iceberg}")
                        except Exception as e:
                            logger.error(f"Error writing batch {batch_count} to Iceberg: {str(e)}")
                            # Optional: add to a failed_batches list or implement retry
                        all_extracted_records_for_batching = [] # Clear batch

        # Write any remaining references after the loop
        if len(all_extracted_records_for_batching) > 0:
            batch_count += 1
            logger.info(f"Writing final batch {batch_count} with {len(all_extracted_records_for_batching)} references to Iceberg table.")
            try:
                final_batch_df = spark.createDataFrame(all_extracted_records_for_batching, schema)
                final_batch_df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                total_records_written_to_iceberg += len(all_extracted_records_for_batching)
                logger.info(f"Successfully wrote final batch {batch_count}. Total records written to Iceberg: {total_records_written_to_iceberg}")
            except Exception as e:
                logger.error(f"Error writing final batch {batch_count} to Iceberg: {str(e)}")
            all_extracted_records_for_batching = []

        total_references_identified = in_network_count + allowed_amount_count + other_count
        logger.info(f"Finished processing index file. Identified {total_references_identified} total references.")
        logger.info(f"Attempted to write {total_records_written_to_iceberg} records to Iceberg across {batch_count} batches.")
        logger.info(f"Final counts: In-network: {in_network_count}, Allowed-amount: {allowed_amount_count}, Other: {other_count}")

        sample_urls_for_output = all_extracted_urls[:20]

        # Save results to output file
        with open(config['output_file'], "w") as f:
            f.write(f"# Anthem File References Summary\n\n")
            f.write(f"Extraction completed: {datetime.now()}\n\n")
            f.write(f"Total references identified: {total_references_identified}\n")
            f.write(f"Total records written to Iceberg: {total_records_written_to_iceberg}\n")
            f.write(f"- In-Network files: {in_network_count}\n")
            f.write(f"- Allowed Amount files: {allowed_amount_count}\n")
            f.write(f"- Other files: {other_count}\n\n")
            f.write(f"## Iceberg Table Information\n")
            f.write(f"- Catalog: {config['catalog_name']}\n")
            f.write(f"- Table: {config['file_refs_table']}\n")
            f.write(f"- Warehouse Location: {config['warehouse_location']}\n\n")
            f.write(f"## Sample URLs (first {len(sample_urls_for_output)})\n")
            for url_item in sample_urls_for_output:
                f.write(f"- {url_item}\n")
        logger.info(f"Summary written to {config['output_file']}")

        with open(config['summary_file'], 'w') as f:
            f.write(f"=== Anthem File References Detailed Summary ===\n")
            f.write(f"Extraction completed: {datetime.now()}\n\n")
            f.write(f"Processing statistics:\n")
            f.write(f"- Total batches processed: {batch_count}\n")
            f.write(f"- Configured batch size: {config['batch_size']}\n")
            f.write(f"- Total references identified from index: {total_references_identified}\n")
            f.write(f"- Total records attempted for Iceberg write: {total_records_written_to_iceberg}\n\n")
            f.write(f"References by network type (from regex matching):\n")
            f.write(f"- In-Network: {in_network_count}\n")
            f.write(f"- Allowed Amount: {allowed_amount_count}\n")
            f.write(f"- Other: {other_count}\n")
        logger.info(f"Detailed summary written to {config['summary_file']}")

        # Prepare data for XCom (TaskFlow returns values)
        xcom_data = {
            'total_file_refs_identified': total_references_identified,
            'total_records_written_iceberg': total_records_written_to_iceberg,
            'in_network_count': in_network_count,
            'allowed_amount_count': allowed_amount_count,
            'other_count': other_count,
            'sample_urls': sample_urls_for_output[:15]  # Limit XCom size for safety
        }
        logger.info(f"Task will return XCom data: {list(xcom_data.keys())}") # Log keys

        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped successfully.")
        
        end_time = time.time() # Define end_time AFTER Spark stop
        elapsed_seconds = end_time - start_time # start_time is defined at the beginning of the function
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        logger.info(f"✅ extract_file_references task complete! Took {hours}h {minutes}m {seconds}s ({elapsed_seconds:.2f}s)")
        logger.info(f"Identified {total_references_identified} references. Attempted to write {total_records_written_to_iceberg} records to Iceberg.")
        
        return xcom_data # Return dictionary of results for XCom

    except Exception as e:
        logger.error(f"❌ Error during file reference extraction's main try block: {str(e)}")
        # Log traceback to the dedicated log file
        with open(config['log_file'], "a") as f:
            f.write(f"ERROR during extract_file_references: {str(e)}\n")
            traceback.print_exc(file=f)
        # Also write a simple error to the main output file for quick visibility in Airflow UI if it reads this file
        with open(config['output_file'], "w") as f:
            f.write(f"ERROR: {str(e)}\nDetails in log file: {config['log_file']}\n")
        
        try:
            # Check if spark is defined and if the SparkContext is available
            if 'spark' in locals() and hasattr(spark, 'sparkContext') and spark.sparkContext._jsc is not None:
                logger.info("Attempting to stop Spark session after error...")
                spark.stop()
                logger.info("Spark session stopped after error.")
            else:
                logger.info("Spark session was not active or already stopped.")
        except Exception as se:
            logger.error(f"Error stopping Spark session after main error: {str(se)}")
            
        raise # Re-raise the exception for Airflow to catch and mark task as failed

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

# Task instances and dependencies are defined using the @dag decorated function below

@dag(
    dag_id='process_anthem_index_file_references',
    default_args=default_args,
    description='Process Anthem price transparency data to Iceberg tables (TaskFlow API)',
    schedule='@monthly',
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['price-transparency', 'anthem', 'iceberg', 'taskflow'],
)
def anthem_processing_tf_dag():
    """Main DAG definition using TaskFlow API."""
    setup_task_output = setup_environment_task()
    extract_file_references_task(setup_task_output) # Pass output if setup_environment_task returned something, or just for dependency

# Instantiate the DAG
anthem_dag_instance = anthem_processing_tf_dag()


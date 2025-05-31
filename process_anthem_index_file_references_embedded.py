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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# Feature flags for the cloud-agnostic approach
# These can be toggled to enable/disable specific features during development and testing
USE_S3_SENSOR = True       # Set to False to skip S3 sensor (use EmptyOperator instead)
USE_S3_FOR_DATA = True     # Set to False to use local files instead of S3
USE_SPARK_ICEBERG = True   # Set to False to skip Iceberg integration

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

# Configuration - Exact parameters from shell script command
config = {
    # Exact S3 paths from shell script command
    'index_file': 's3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz',
    'warehouse_location': 's3a://price-transparency-raw/warehouse',
    'catalog_name': 'anthem_catalog',
    'file_refs_table': 'anthem_file_refs',
    
    # Output configuration
    'output_dir': "/home/airflow/anthem-processing",
    'output_file': "/home/airflow/anthem-processing/file_references.txt",
    'log_file': "/home/airflow/anthem-processing/extraction_log.txt",
    'summary_file': "/home/airflow/anthem-processing/summary.txt",
    
    # Processing configuration
    'max_chunks': None,     # Process the entire file - will take hours
    'batch_size': 10000,    # Batch size for Iceberg writes
    
    # S3 parameters for Airflow operators
    'bucket_name': 'price-transparency-raw', 
    'index_file_key': 'payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz',
    
    # JAR files configuration - exactly like shell script
    'jar_dir': '/home/airflow/jars',
    'jar_files': {
        'iceberg-spark': {
            'path': '/home/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
            'url': 'https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar'
        },
        'hadoop-aws': {
            'path': '/home/airflow/jars/hadoop-aws-3.3.4.jar',
            'url': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar'
        },
        'aws-java-sdk': {
            'path': '/home/airflow/jars/aws-java-sdk-bundle-1.12.262.jar',
            'url': 'https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar'
        }
    }
}

# Task 1: Check if the index file exists - use EmptyOperator when S3 access isn't needed
if USE_S3_SENSOR:
    # Standard S3 approach when S3 is available
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
    # Cloud-agnostic approach: use EmptyOperator when not using S3 sensor
    # This allows the DAG to run in environments without S3 access
    check_index_file = EmptyOperator(
        task_id='check_index_file',
        dag=dag
    )

# Task 2: Setup the environment and prepare for data processing
def init_spark(**context):
    """Initialize Spark session with Iceberg support exactly like the shell script."""
    import logging
    import os
    
    logger = logging.getLogger('airflow.task')
    logger.info(f"Initializing Spark with Iceberg support...")
    logger.info(f"Warehouse location: {config['warehouse_location']}")
    
    # Make sure environment variables are set just like in the shell script
    jars_path = ",".join(config['jar_paths'])
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {jars_path} pyspark-shell'
    
    # Create the Spark session with exact same configs as the shell script
    spark = (SparkSession.builder
             .appName("Anthem File References Extractor")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config(f"spark.sql.catalog.{config['catalog_name']}", "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{config['catalog_name']}.type", "hadoop")
             .config(f"spark.sql.catalog.{config['catalog_name']}.warehouse", config['warehouse_location'])
             .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
             .config("spark.sql.catalog.spark_catalog.type", "hive")
             .config("spark.sql.parquet.compression.codec", "zstd")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .getOrCreate())
    
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


def setup_environment(**context):
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
    
    # Test S3 connectivity
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
            logger.error(error_msg)
            with open(config['log_file'], 'a') as f:
                f.write(f"ERROR: {error_msg}\n")
            raise RuntimeError(error_msg)
    except Exception as e:
        error_msg = f"S3 connectivity test failed: {str(e)}"
        logger.error(error_msg)
        with open(config['log_file'], 'a') as f:
            f.write(f"ERROR: {error_msg}\n")
        raise RuntimeError(error_msg)
    
    logger.info("Environment setup completed successfully")
    return True

# Task 3: Extract file references and save to output file
def extract_file_references(**context):
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
        with open(config['log_file'], "a") as f:
            f.write(f"ERROR: {error_msg}\n")
            traceback.print_exc(file=f)
        raise RuntimeError(error_msg)
    
    # File reference patterns
    URL_PATTERNS = [
        r'"in_network_files"\s*:\s*\[\s*"([^"]+)"',
        r'"allowed_amount_files"\s*:\s*\[\s*"([^"]+)"',
        r'"url"\s*:\s*"([^"]+\.(?:json|csv|parquet|gz))"',
        r'https?://[^"\s]+\.(?:json|csv|parquet|gz)'
    ]
    try:
        # Extract schema information to create proper DataFrame
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
        
        # Get index file information
        logger.info(f"Processing index file: {config['index_file']}")
        
        # Extract file references using direct streaming to be memory-efficient
        # This cloud-agnostic approach works with any storage system that supports
        # streaming (S3, GCS, Azure Blob, or local filesystem)
        
        # Parse URL to determine access method
        parsed_url = urlparse(config['index_file'])
        
        # Ensure we track statistics for reporting
        total_records = 0
        in_network_count = 0
        allowed_amount_count = 0
        other_count = 0
        batch_count = 0
        sample_urls = []
        
        # Using standard Hadoop file system API which is cloud-agnostic
        # This will work with s3a, gs, wasbs, and local file systems
        logger.info(f"Reading index file using Spark (cloud-agnostic approach)")
        
        # Use Spark to read the file for better memory management of the large file
        # This uses Hadoop's FileSystem API which supports multiple cloud providers
        df = spark.read.text(config['index_file'])
        
        # Initialize references list and batch counter
        references = []
        logger.info(f"Processing index file content - this will take several hours")
        
        # Process file line by line to extract file references
        for row in df.collect():
            line = row[0]
            
            # Apply each URL pattern to find references
            for pattern in url_patterns:
                matches = re.findall(pattern, line)
                
                # Process each match
                for match in matches:
                    # Handle the case where the pattern has a capture group
                    url = match[0] if isinstance(match, tuple) else match
                    
                    # Filter out any non-HTTP URLs
                    if not url.startswith('http'):
                        continue
                    
                    # Add to our list
                    references.append(url)
                    
                    # Keep track of a few sample URLs for logging
                    if len(sample_urls) < 10:
                        sample_urls.append(url)
                    
                    # Determine the file type for categorization
                    file_network = "other"
                    file_type = "unknown"
                    
                    if "in-network" in url.lower():
                        file_network = "in-network"
                        in_network_count += 1
                    elif "allowed-amount" in url.lower():
                        file_network = "allowed-amount"
                        allowed_amount_count += 1
                    else:
                        other_count += 1
                    
                    # Determine file type
                    if url.endswith(".json"):
                        file_type = "json"
                    elif url.endswith(".csv"):
                        file_type = "csv"
                    elif url.endswith(".gz"):
                        if ".json.gz" in url.lower():
                            file_type = "json.gz"
                        elif ".csv.gz" in url.lower():
                            file_type = "csv.gz"
                        else:
                            file_type = "gz"
                    
                    # Check if we have enough for a batch write
                    if len(references) >= config['batch_size']:
                        # Write batch to Iceberg table
                        batch_count += 1
                        total_records += len(references)
                        
                        logger.info(f"Writing batch {batch_count} with {len(references)} references to Iceberg table")
                        logger.info(f"Stats so far: In-network: {in_network_count}, Allowed-amount: {allowed_amount_count}, Other: {other_count}")
                        
                        # Create data for DataFrame - standard Spark code that works with any cloud
                        data = []
                        for ref in references:
                            data.append({
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
                        
                        # Create and write the DataFrame using standard Iceberg APIs
                        # This approach is cloud-provider independent
                        try:
                            df = spark.createDataFrame(data, schema)
                            df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                            
                            logger.info(f"Successfully wrote batch {batch_count}")
                        except Exception as e:
                            error_msg = f"Error writing batch {batch_count}: {str(e)}"
                            logger.error(error_msg)
                            with open(config['log_file'], 'a') as f:
                                f.write(f"ERROR: {error_msg}\n")
                        
                        # Reset references list for next batch
                        references = []
        
        # Write any remaining references before finishing
        if len(references) > 0:
            batch_count += 1
            total_records += len(references)
            
            logger.info(f"Writing final batch {batch_count} with {len(references)} references to Iceberg table")
            
            # Create data for DataFrame - standard Spark code that works with any cloud
            data = []
            for ref in references:
                # Determine the file type for categorization
                file_network = "other"
                file_type = "unknown"
                
                if "in-network" in ref.lower():
                    file_network = "in-network"
                elif "allowed-amount" in ref.lower():
                    file_network = "allowed-amount"
                
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
                data.append({
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
            
            # Create and write the DataFrame using standard Iceberg APIs
            # This approach is cloud-provider independent
            try:
                df = spark.createDataFrame(data, schema)
                df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                
                logger.info(f"Successfully wrote final batch")
            except Exception as e:
                error_msg = f"Error writing final batch: {str(e)}"
                logger.error(error_msg)
                with open(config['log_file'], 'a') as f:
                    f.write(f"ERROR: {error_msg}\n")
                    
        # Get final statistics from Iceberg table - using standard Spark SQL
        # This is a cloud-agnostic approach that works with any storage backend
        logger.info("Getting final statistics from Iceberg table...")
        try:
            # Use Spark SQL to query the Iceberg table
            stats_df = spark.sql(f"""
                SELECT 
                    file_network,
                    COUNT(*) as count 
                FROM {config['catalog_name']}.{config['file_refs_table']} 
                GROUP BY file_network
            """)
            
            # Process results
            total_count = 0
            stats = {}
            
            for row in stats_df.collect():
                network = row['file_network']
                count = row['count']
                stats[network] = count
                total_count += count
                
            logger.info(f"Final statistics from Iceberg table:")
            logger.info(f"Total references: {total_count}")
            for network, count in stats.items():
                logger.info(f"{network}: {count}")
                
            # Sample some URLs from the table
            sample_df = spark.sql(f"""
                SELECT file_network, file_url 
                FROM {config['catalog_name']}.{config['file_refs_table']} 
                LIMIT 10
            """)
            
            sample_urls = [row['file_url'] for row in sample_df.collect()]            
        except Exception as e:
            error_msg = f"Error getting statistics from Iceberg table: {str(e)}"
            logger.error(error_msg)
            with open(config['log_file'], 'a') as f:
                f.write(f"ERROR: {error_msg}\n")
        
        # Write the output summary file - cloud-agnostic approach using standard file I/O
        with open(config['output_file'], 'w') as f:
            f.write(f"=== Anthem File References Summary ===\n")
            f.write(f"Extraction completed: {datetime.now()}\n\n")
            f.write(f"Total references extracted: {total_count}\n")
            
            # Write stats by network type
            f.write(f"\nReferences by network type:\n")
            for network, count in stats.items() if 'stats' in locals() else {}:
                f.write(f"- {network}: {count}\n")
            
            # Write sample URLs
            f.write(f"\nSample URLs:\n")
            for i, url in enumerate(sample_urls[:10]):
                f.write(f"{i+1}. {url}\n")
            
            # Write processing information
            f.write(f"\nProcessing information:\n")
            f.write(f"- Index file: {config['index_file']}\n")
            f.write(f"- Iceberg table: {config['catalog_name']}.{config['file_refs_table']}\n")
            f.write(f"- Warehouse location: {config['warehouse_location']}\n")
            
            # Add cloud-agnostic note to emphasize the approach used
            f.write(f"\nNote: This extraction used a cloud-agnostic approach with standard Hadoop FileSystem\n")
            f.write(f"API and Iceberg for maximum portability across different environments.\n")
        
        # Write a detailed summary file with more information
        with open(config['summary_file'], 'w') as f:
            f.write(f"=== Anthem File References Detailed Summary ===\n")
            f.write(f"Extraction completed: {datetime.now()}\n\n")
            
            # Write processing stats
            f.write(f"Processing statistics:\n")
            f.write(f"- Total batches processed: {batch_count}\n")
            f.write(f"- Batch size: {config['batch_size']}\n")
            f.write(f"- Total references: {total_count}\n\n")
            
            # Write detailed stats by file type if available
            try:
                type_stats_df = spark.sql(f"""
                    SELECT 
                        file_type,
                        COUNT(*) as count 
                    FROM {config['catalog_name']}.{config['file_refs_table']} 
                    GROUP BY file_type
                    ORDER BY count DESC
                """)
                
                f.write(f"References by file type:\n")
                for row in type_stats_df.collect():
                    f.write(f"- {row['file_type']}: {row['count']}\n")
            except Exception as e:
                f.write(f"Error getting file type statistics: {e}\n")
        
        # Stop Spark session - important for resource cleanup
        logger.info("Stopping Spark session...")
        try:
            spark.stop()
            logger.info("Spark session stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")
        
        # Calculate and log execution time
        end_time = time.time()
        elapsed_seconds = end_time - start_time
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        
        logger.info(f"Extraction completed in {hours}h {minutes}m {seconds}s")
        logger.info(f"Processed {total_count} file references")
        logger.info(f"Results written to {config['output_file']} and {config['summary_file']}")
        
        # Push results to XCom for downstream tasks
        context['ti'].xcom_push(key='total_file_refs', value=total_count)
        context['ti'].xcom_push(key='sample_urls', value=sample_urls[:10])
        
        return total_count
    except Exception as e:
        error_msg = f"Error in extract_file_references: {str(e)}"
        logger.error(error_msg)
        
        # Write error to log file
        with open(config['log_file'], 'a') as f:
            f.write(f"ERROR: {error_msg}\n")
            traceback.print_exc(file=f)
        
        # Write error to output file
        with open(config['output_file'], 'w') as f:
            f.write(f"ERROR: {error_msg}\n")
        
        # Clean up Spark session if it exists
        try:
            if 'spark' in locals():
                spark.stop()
                logger.info("Spark session stopped after error")
        except Exception as cleanup_error:
            logger.error(f"Error stopping Spark session: {cleanup_error}")
        
        # Re-raise the exception for Airflow to handle
        raise RuntimeError(error_msg)
        
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
                    # Process batch
                    batch_count += 1
                    
                    # Only write to Iceberg if enabled - cloud-agnostic approach
                    if USE_SPARK_ICEBERG and spark is not None:
                        log_message(f"💾 Processing batch {batch_count}: Writing {len(references)} references to Iceberg table...")
                    else:
                        log_message(f"Processing batch {batch_count}: Found {len(references)} references (not writing to Iceberg)...")
                    
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
                    
                    # Track counts for reporting - this always happens regardless of Iceberg integration
                    for ref in references:
                        if "in-network" in ref.lower():
                            in_network_count += 1
                        elif "allowed-amount" in ref.lower():
                            allowed_amount_count += 1
                        else:
                            other_count += 1
                    
                    # Only write to Iceberg if enabled - cloud-agnostic approach
                    if USE_SPARK_ICEBERG and spark is not None:
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
                    else:
                        # Just log the counts when not using Iceberg - cloud-agnostic approach for testing
                        log_message(f"Processed batch {batch_count} with {len(references)} references (in-network: {in_network_count}, allowed-amount: {allowed_amount_count}, other: {other_count})")
                        log_message(f"Sample URL: {references[0] if references else '(none)'}")
                    
                    
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
            # Only mention Iceberg if we're using it - cloud-agnostic approach
            if USE_SPARK_ICEBERG and spark is not None:
                log_message(f"💾 Processing final batch {batch_count}: Writing {len(references)} remaining references to Iceberg table...")
            else:
                log_message(f"Processing final batch {batch_count}: Found {len(references)} remaining references (not writing to Iceberg)...")
            
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
            
            # Track counts for reporting - this always happens regardless of Iceberg integration
            for ref in references:
                if "in-network" in ref.lower():
                    in_network_count += 1
                elif "allowed-amount" in ref.lower():
                    allowed_amount_count += 1
                else:
                    other_count += 1
                    
            # Only write to Iceberg if enabled - cloud-agnostic approach
            if USE_SPARK_ICEBERG and spark is not None:
                try:
                    # Create DataFrame
                    df = spark.createDataFrame(file_refs, schema)
                    
                    # Write to Iceberg table in append mode
                    df.writeTo(f"{config['catalog_name']}.{config['file_refs_table']}").append()
                    
                    log_message(f"✅ Successfully wrote final batch to Iceberg table")
                except Exception as e:
                    log_message(f"❌ Error writing final batch to Iceberg: {e}")
                    traceback.print_exc(file=open(config['log_file'], "a"))
            else:
                # Just log the counts when not using Iceberg - cloud-agnostic approach for testing
                log_message(f"Processed final batch with {len(references)} references (in-network: {in_network_count}, allowed-amount: {allowed_amount_count}, other: {other_count})")
                log_message(f"Sample URL: {references[0] if references else '(none)'}")
            
        
        # Only query Iceberg if we're using it - cloud-agnostic approach
        total_count = in_network_count + allowed_amount_count + other_count
        sample_urls = references[:20] if references else []
        
        if USE_SPARK_ICEBERG and spark is not None:
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
                log_message("Falling back to local statistics")
        else:
            # Using local statistics - cloud-agnostic approach for testing
            log_message(f"📊 Using local statistics (Iceberg integration disabled)")
            log_message(f"📊 Total references: {total_count}")
            log_message(f"📊 Categories: {in_network_count} in-network, {allowed_amount_count} allowed-amount, {other_count} other")
        
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
    """Verify the extracted file references from the output file.
    
    This function is cloud-agnostic and works whether Iceberg is used or not.
    """
    import os
    
    output_file = config['output_file']
    
    # Check if the output file exists
    if not os.path.exists(output_file):
        print(f"Warning: Output file not found: {output_file}")
        # Don't fail - this might be a development run
        if not USE_SPARK_ICEBERG:
            print(f"Spark/Iceberg integration is disabled (USE_SPARK_ICEBERG=False), continuing anyway")
            return 0
        else:
            raise ValueError(f"Output file not found: {output_file}")
    
    # Read and analyze the file
    with open(output_file, 'r') as f:
        content = f.read()
    
    # Check for errors in the output
    if content.startswith("ERROR:"):
        error_msg = content.split('\n')[0].strip()
        # Don't fail if the error is just Spark initialization and we're in development mode
        if "Spark initialization failed" in error_msg and not USE_SPARK_ICEBERG:
            print(f"Warning: {error_msg}")
            print(f"Spark/Iceberg integration is disabled (USE_SPARK_ICEBERG=False), continuing anyway")
            return 0
        else:
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

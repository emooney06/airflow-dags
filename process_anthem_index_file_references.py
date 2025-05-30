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
    
    # Environment setup
    'jars_path': '/home/ec2-user/iceberg-jars:/home/ec2-user/hadoop-aws-jars',
    
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
# For development/testing purposes, use the PythonOperator instead of BashOperator
# This allows us to mock the extraction in the Airflow environment

def mock_extract_file_references(**context):
    """Mock extraction function for Airflow development/testing"""
    import time
    import random
    
    # Log the parameters we would use
    s3_path = f"s3a://{config['s3_bucket']}/{config['index_file_key']}"
    warehouse = config['warehouse_location']
    catalog = config['catalog_name']
    table = config['file_refs_table']
    
    print(f"📋 Extracting file references from: {s3_path}")
    print(f"📋 Writing to Iceberg table: {catalog}.{table}")
    print(f"📋 Warehouse location: {warehouse}")
    
    # Simulate processing time
    total_refs = 0
    for i in range(5):
        # Simulate finding references
        refs_found = random.randint(5000, 15000)
        total_refs += refs_found
        print(f"💾 Batch {i+1}: Found {refs_found} file references...")
        time.sleep(2)  # Small delay for logging to appear
    
    print(f"✅ Extraction complete! Processed {total_refs} file references")
    
    # Push metrics to XCom
    context['ti'].xcom_push(key='total_file_refs', value=total_refs)
    context['ti'].xcom_push(key='extraction_successful', value=True)
    
    return total_refs

# Use PythonOperator for the mock extraction
from airflow.operators.python import PythonOperator

extract_file_refs = PythonOperator(
    task_id='extract_file_references',
    python_callable=mock_extract_file_references,
    dag=dag
)

# Comment out the original BashOperator for now
'''
BashOperator version - for use on actual EC2 instances with proper setup:

extract_file_refs_script = """
#!/bin/bash
# Create a local directory for extraction scripts
mkdir -p /tmp/anthem-processing

# Set environment variables for PySpark
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 pyspark-shell"

INDEX_FILE="{{ params.s3_path }}"
WAREHOUSE="{{ params.warehouse }}"
CATALOG="{{ params.catalog }}"
TABLE="{{ params.table }}"
BATCH_SIZE="{{ params.batch_size }}"
{% if params.max_records %}
MAX_RECORDS="--max-records {{ params.max_records }}"
{% else %}
MAX_RECORDS=""
{% endif %}

# Create a simple test extraction script
cat > /tmp/anthem-processing/test_extraction.py << 'EOF'
import time
import random
import sys

def mock_extract(index_file, warehouse, catalog, table):
    """Mock extraction for testing the Airflow pipeline"""
    print(f"📋 Extracting file references from: {index_file}")
    print(f"📋 Writing to Iceberg table: {catalog}.{table}")
    print(f"📋 Warehouse location: {warehouse}")
    
    # Simulate processing
    total_refs = 0
    for i in range(5):
        refs_found = random.randint(5000, 15000)
        total_refs += refs_found
        print(f"💾 Batch {i+1}: Found {refs_found} file references...")
        time.sleep(1)
    
    print(f"✅ Extraction complete! Processed {total_refs} file references")
    return total_refs

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-file", required=True)
    parser.add_argument("--warehouse-location", required=True)
    parser.add_argument("--catalog-name", required=True)
    parser.add_argument("--file-refs-table", required=True)
    args = parser.parse_args()
    
    mock_extract(
        args.index_file,
        args.warehouse_location,
        args.catalog_name,
        args.file_refs_table
    )
EOF

# Run the mock extraction
python3 /tmp/anthem-processing/test_extraction.py \
  --index-file "$INDEX_FILE" \
  --warehouse-location "$WAREHOUSE" \
  --catalog-name "$CATALOG" \
  --file-refs-table "$TABLE"
"""

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
'''

# Task 4: Verify extraction results
def verify_extraction(**context):
    """Verify the extracted file references and log summary statistics."""
    from pyspark.sql import SparkSession
    import findspark
    findspark.init()

    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("Verify Anthem Extraction")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config(f"spark.sql.catalog.{config['catalog_name']}", "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{config['catalog_name']}.type", "hadoop")
             .config(f"spark.sql.catalog.{config['catalog_name']}.warehouse", config['warehouse_location'])
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
             .getOrCreate())

    # Get row count
    count_df = spark.sql(f"SELECT COUNT(*) as total_refs FROM {config['catalog_name']}.{config['file_refs_table']}")
    total_refs = count_df.collect()[0]['total_refs']
    
    # Get distribution by file type
    type_distribution = spark.sql(f"""
    SELECT file_type, COUNT(*) as count 
    FROM {config['catalog_name']}.{config['file_refs_table']} 
    GROUP BY file_type 
    ORDER BY count DESC
    """).collect()
    
    # Log results
    context['ti'].xcom_push(key='total_file_refs', value=total_refs)
    context['ti'].xcom_push(key='type_distribution', value=str(type_distribution))
    
    # Log sample URLs
    sample_urls = spark.sql(f"""
    SELECT file_type, file_url 
    FROM {config['catalog_name']}.{config['file_refs_table']} 
    ORDER BY file_type 
    LIMIT 5
    """).collect()
    
    context['ti'].xcom_push(key='sample_urls', value=str(sample_urls))
    
    print(f"✅ Extraction verification complete. Found {total_refs} total file references.")
    for row in type_distribution:
        print(f"  - {row['file_type']}: {row['count']} files")
    
    spark.stop()
    
    # If we didn't find any references, fail the task
    if total_refs == 0:
        raise ValueError("No file references found! Check the extraction process.")
    
    return total_refs

verify_task = PythonOperator(
    task_id='verify_extraction',
    python_callable=verify_extraction,
    dag=dag
)

# Task 5: Process individual files (could be expanded into a dynamic task group)
process_files_script = """
#!/bin/bash
# This is a placeholder for the actual processing of individual files
# In a complete implementation, you'd loop through file references and process each

export PYSPARK_SUBMIT_ARGS="--jars /home/ec2-user/hadoop-aws-jars/hadoop-aws-3.3.4.jar,/home/ec2-user/hadoop-aws-jars/aws-java-sdk-bundle-1.12.262.jar,/home/ec2-user/hadoop-aws-jars/hadoop-common-3.3.4.jar,/home/ec2-user/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/home/ec2-user/iceberg-jars/iceberg-aws-bundle-1.4.2.jar --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 pyspark-shell"

# Get a sample of files to process (for testing)
python - << EOF
import findspark
findspark.init()

from pyspark.sql import SparkSession
import os

warehouse = "${params.warehouse}"
catalog = "${params.catalog}"
file_refs_table = "${params.file_refs_table}"
processed_table = "${params.processed_table}"

spark = (SparkSession.builder
       .appName("Anthem File Processor")
       .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
       .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
       .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
       .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
       .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
       .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
       .getOrCreate())

# Get sample of files to process
sample = spark.sql(f"SELECT file_type, file_url FROM {catalog}.{file_refs_table} LIMIT 10").collect()

print(f"Would process {len(sample)} files:")
for row in sample:
    print(f"  - {row['file_type']}: {row['file_url']}")

# In a real implementation, you would:
# 1. Create target tables if needed
# 2. Process each file based on its type
# 3. Write the results to Iceberg tables

spark.stop()
EOF
"""

process_files = BashOperator(
    task_id='process_files',
    bash_command=process_files_script,
    params={
        'warehouse': config['warehouse_location'],
        'catalog': config['catalog_name'],
        'file_refs_table': config['file_refs_table'],
        'processed_table': config['processed_table']
    },
    dag=dag
)

# Define task dependencies
check_index_file >> setup_env >> extract_file_refs >> verify_task >> process_files

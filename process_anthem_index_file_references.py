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

# Write the simplest possible Python script - no multiline strings at all
echo 'import os
print("Starting extraction...")
output_file = "/home/airflow/anthem_file_references.txt"

try:
    with open(output_file, "w") as f:
        f.write("Test file created successfully")
    print("File created successfully")
except Exception as e:
    print("Error:", str(e))
' > /home/airflow/anthem-processing/simple.py

# Run the script
python3 /home/airflow/anthem-processing/simple.py

# Show the results
echo "=== EXTRACTION RESULTS ==="
cat /home/airflow/anthem_file_references.txt || echo "Could not display output file"
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
    
    output_file = "/home/airflow/anthem_file_references.txt"
    
    # Check if the output file exists
    if not os.path.exists(output_file):
        raise ValueError(f"Output file not found: {output_file}")
    
    # Read and analyze the file
    with open(output_file, 'r') as f:
        content = f.read()
    
    # Extract basic statistics
    first_line = content.split('\n')[0]
    total_refs = 0
    if "Found " in first_line and " file references" in first_line:
        try:
            total_refs = int(first_line.split("Found ")[1].split(" file")[0])
        except:
            total_refs = len(content.split('\n')) - 1  # Estimate from line count
    
    # Count by type
    in_network_count = content.count('in-network')
    allowed_amount_count = content.count('allowed-amount')
    other_count = total_refs - in_network_count - allowed_amount_count
    
    # Log results
    context['ti'].xcom_push(key='total_file_refs', value=total_refs)
    context['ti'].xcom_push(key='file_content', value=content[:1000])  # First 1000 chars
    
    print(f"✅ Extraction verification complete. Found {total_refs} total file references.")
    print(f"  - In-network files: ~{in_network_count}")
    print(f"  - Allowed amount files: ~{allowed_amount_count}")
    print(f"  - Other files: ~{other_count}")
    
    # Get sample URLs
    sample_urls = []
    for line in content.split('\n')[1:11]:  # Get up to 10 sample URLs
        if ". " in line:
            url = line.split(". ", 1)[1].strip()
            sample_urls.append(url)
    
    context['ti'].xcom_push(key='sample_urls', value=str(sample_urls))
    
    # Print sample URLs
    print(f"\nSample URLs (first 3):")
    for i, url in enumerate(sample_urls[:3]):
        print(f"  {i+1}. {url}")
    
    # If we didn't find any references, fail the task
    if total_refs == 0:
        raise ValueError("No file references found! Check the extraction process.")
    
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
    
    print(f"✅ Found {total_refs} file references for processing")
    
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
    
    print("\nNext steps would be to:")
    print("1. Download each file from its URL")
    print("2. Parse the content based on file type (JSON, CSV, etc.)")
    print("3. Transform the data into a standard format")
    print("4. Write to Iceberg tables for analytics")
    
    return total_refs

process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    dag=dag
)

# Define task dependencies
check_index_file >> setup_env >> extract_file_refs >> verify_task >> process_files_task

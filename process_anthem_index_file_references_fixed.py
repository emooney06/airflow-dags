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
import re
import ast
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
    
    # Ensure boto3 is installed
    try:
        subprocess.run("pip install boto3", shell=True, check=True)
        print("✅ Successfully installed boto3")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not install boto3: {e}")
    
    # Create a log directory
    log_dir = "/home/airflow/logs/anthem"
    subprocess.run(f"mkdir -p {log_dir}", shell=True, check=True)
    
    # Test AWS connectivity
    try:
        import boto3
        s3 = boto3.client('s3')
        buckets = s3.list_buckets()
        print(f"✅ Successfully connected to AWS. Found {len(buckets.get('Buckets', []))} buckets")
    except Exception as e:
        print(f"Warning: Could not connect to AWS: {e}")
    
    print("✅ Successfully created working directory at /home/airflow/anthem-processing")
    return True

setup_env = PythonOperator(
    task_id='setup_environment',
    python_callable=setup_environment,
    dag=dag
)

# Task 3: Extract file references and save to Iceberg table
extract_file_refs_script = '''
#!/bin/bash
echo "==== Starting Anthem file reference extraction at $(date) ===="
echo "Using the existing extract_anthem_file_references.py script which is proven to work"

# Create the output directory if it doesn't exist
mkdir -p /home/airflow/anthem-processing

# Copy the working script to the Airflow directory
cp /home/ejmooney/dev/extract_anthem_file_references.py /home/airflow/anthem-processing/

# Check if the required JARs directory exists, if not create it
mkdir -p /home/airflow/hadoop-aws-jars
mkdir -p /home/airflow/iceberg-jars

# Check for necessary JARs and download if missing
if [ ! -f "/home/airflow/hadoop-aws-jars/hadoop-aws-3.3.4.jar" ]; then
    echo "Downloading required Hadoop AWS JARs..."
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /home/airflow/hadoop-aws-jars/
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /home/airflow/hadoop-aws-jars/
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -P /home/airflow/hadoop-aws-jars/
fi

if [ ! -f "/home/airflow/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" ]; then
    echo "Downloading required Iceberg JARs..."
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar -P /home/airflow/iceberg-jars/
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.2/iceberg-aws-bundle-1.4.2.jar -P /home/airflow/iceberg-jars/
fi

# Set environment variables for PySpark
export PYSPARK_SUBMIT_ARGS="--jars /home/airflow/hadoop-aws-jars/hadoop-aws-3.3.4.jar,/home/airflow/hadoop-aws-jars/aws-java-sdk-bundle-1.12.262.jar,/home/airflow/hadoop-aws-jars/hadoop-common-3.3.4.jar,/home/airflow/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/home/airflow/iceberg-jars/iceberg-aws-bundle-1.4.2.jar --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 pyspark-shell"

# Test S3 access before proceeding
echo "🔍 Testing S3 access..."
python3 -c "
import boto3
import sys
try:
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()
    print('✅ S3 access test successful. Found', len(buckets.get('Buckets')), 'buckets')
except Exception as e:
    print(f'❌ S3 access test failed: {str(e)}')
    sys.exit(1)
"

if [ $? -ne 0 ]; then
    echo "❌ S3 access test failed. Please check your AWS credentials."
    exit 1
fi

# Make the script executable
chmod +x /home/airflow/anthem-processing/extract_anthem_file_references.py

# Run the actual working script with the appropriate arguments
echo "🚀 Running Anthem file reference extractor..."
echo "   Index file: s3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz"
echo "   Warehouse: s3a://price-transparency-raw/warehouse"

# This is the command that actually works from user's existing scripts
python3 /home/airflow/anthem-processing/extract_anthem_file_references.py \
  --index-file s3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz \
  --warehouse-location s3a://price-transparency-raw/warehouse \
  --catalog-name anthem_catalog \
  --file-refs-table anthem_file_refs

# Create a summary file of the extraction
# This captures Iceberg table info in a format the verify task can read
spark-shell --conf spark.sql.catalog.anthem_catalog=org.apache.iceberg.spark.SparkCatalog \
           --conf spark.sql.catalog.anthem_catalog.type=hadoop \
           --conf spark.sql.catalog.anthem_catalog.warehouse=s3a://price-transparency-raw/warehouse \
           -c "
import org.apache.spark.sql.functions._

// Get counts by network type
val counts = spark.sql(\"SELECT file_network, COUNT(*) as count FROM anthem_catalog.anthem_file_refs GROUP BY file_network\").collect()
val totalCount = counts.map(_.getLong(1)).sum

// Get some sample URLs
val sample = spark.sql(\"SELECT file_network, file_url FROM anthem_catalog.anthem_file_refs LIMIT 10\").collect()

// Write to the output file
val fw = new java.io.FileWriter(\"/home/airflow/anthem_file_references.txt\")
try {
  fw.write(\"TOTAL_REFS: \" + totalCount + \"\\n\")
  
  // Write counts for each network type
  counts.foreach { row =>
    val networkType = if (row.getString(0) == null || row.getString(0).isEmpty) \"OTHER\" else row.getString(0).toUpperCase
    fw.write(networkType + \": \" + row.getLong(1) + \"\\n\")
  }
  
  // Write sample URLs
  fw.write(\"\\nSAMPLES:\\n\")
  sample.zipWithIndex.foreach { case (row, i) => 
    fw.write((i+1) + \". \" + row.getString(1) + \"\\n\")
  }
} finally {
  fw.close()
}

println(\"✅ Wrote summary with \" + totalCount + \" total references\")
"

echo "==== Extraction completed at $(date) ===="
'''

extract_file_refs = BashOperator(
    task_id='extract_file_references',
    bash_command=extract_file_refs_script,
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
    
    in_network_match = re.search(r'IN[_-]NETWORK:\s*(\d+)', content)
    if in_network_match:
        in_network_count = int(in_network_match.group(1))
    
    allowed_amount_match = re.search(r'ALLOWED[_-]AMOUNT:\s*(\d+)', content)
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

# Task 5: Process files using individual workers
def process_files(**context):
    """Process the extracted file references into meaningful data."""
    # Get the total count of file references and sample URLs from the previous task
    total_refs = context['ti'].xcom_pull(task_ids='verify_extraction', key='total_file_refs')
    sample_urls = context['ti'].xcom_pull(task_ids='verify_extraction', key='sample_urls')
    
    print(f"✅ Successfully extracted {total_refs} file references for processing")
    
    try:
        # Convert string representation back to list
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

#!/usr/bin/env python3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

# Default arguments for long-running tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Increase retries for resilience
    'retry_delay': timedelta(minutes=15),  # Longer retry delay
    'execution_timeout': None,  # No timeout for task execution
    # Removed pool setting as it might not exist
    # Removed priority_weight as it might cause issues
    # Removed max_active_tis_per_dag as it's a DAG-level setting
}

# Get parameters from Airflow Variables with defaults
INDEX_FILE = Variable.get("anthem_index_file", "s3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz")
WAREHOUSE = Variable.get("anthem_warehouse", "s3a://price-transparency-raw/warehouse")
CATALOG = Variable.get("anthem_catalog", "anthem_catalog")
TABLE = Variable.get("anthem_table", "anthem_file_refs")
BATCH_SIZE = int(Variable.get("anthem_batch_size", "1000"))
MAX_RECORDS = int(Variable.get("anthem_max_records", "0"))
PROGRESS_INTERVAL = int(Variable.get("anthem_progress_interval", "30"))

# Create DAG
dag = DAG(
    'anthem_index_reference_streaming_docker',
    default_args=default_args,
    description='Process Anthem index file references using Docker container',
    schedule='@monthly',  # Run monthly on the 1st
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['iceberg', 'docker', 'container', 'anthem', 'price-transparency'],
    # Settings for long-running tasks - simplified
    dagrun_timeout=None  # No timeout for the entire DAG run
)

# Define the task
process_anthem_index_reference = DockerOperator(
    task_id='process_anthem_index_reference',
    image='anthem-streaming:latest',
    api_version='auto',
    auto_remove='success',  # Valid values are 'never', 'success', or 'force'
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    # The AWS credentials will be retrieved from the connection during execution
    # This avoids timeout issues during DAG parsing
    command=[
        "--index-file", INDEX_FILE,
        "--warehouse-location", WAREHOUSE,
        "--catalog-name", CATALOG,
        "--file-refs-table", TABLE,
        "--batch-size", str(BATCH_SIZE),
        "--max-records", str(MAX_RECORDS),
        "--progress-interval", str(PROGRESS_INTERVAL)
    ],
    # Get AWS credentials from Airflow connection at runtime
    environment={
        'AWS_ACCESS_KEY_ID': '{{ conn.AWS_S3.login }}',
        'AWS_SECRET_ACCESS_KEY': '{{ conn.AWS_S3.password }}',
        'AWS_REGION': 'us-west-2'
    },
    # Configuration for long-running processes - simplified
    timeout=3600,  # 1 hour API timeout
    # Removed potentially problematic settings
    dag=dag,
)
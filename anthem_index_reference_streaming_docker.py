from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get configuration from Airflow Variables (or set defaults)
INDEX_FILE = Variable.get("anthem_index_file", 
                         "s3a://price-transparency-raw/payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz")
WAREHOUSE = Variable.get("anthem_warehouse", "s3a://price-transparency-raw/warehouse")
CATALOG = Variable.get("anthem_catalog", "anthem_catalog")
TABLE = Variable.get("anthem_table", "anthem_file_refs")
BATCH_SIZE = int(Variable.get("anthem_batch_size", "1000"))
MAX_RECORDS = int(Variable.get("anthem_max_records", "0"))  # 0 means process all records
PROGRESS_INTERVAL = int(Variable.get("anthem_progress_interval", "30"))

# Create DAG
dag = DAG(
    'anthem_index_reference_streaming_docker',
    default_args=default_args,
    description='Process Anthem index file references using containerized Spark/Iceberg',
    schedule='@monthly',
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['anthem', 'spark', 'iceberg', 'container', 'docker'],
)


def get_task_instance_context(**context):
    aws_hook = AwsBaseHook(aws_conn_id='AWS_S3')
    credentials = aws_hook.get_credentials()
    
    return DockerOperator(
        # ... other parameters ...
        environment={
            'AWS_ACCESS_KEY_ID': credentials.login,
            'AWS_SECRET_ACCESS_KEY': credentials.password,
            'AWS_REGION': 'us-west-2',
        },
    )
# Get AWS credentials from Airflow Variables
# aws_access_key = Variable.get("aws_access_key_id", "")
# aws_secret_key = Variable.get("aws_secret_access_key", "")
# aws_region = Variable.get("aws_region", "us-west-2")

# Define the task
process_anthem_index_reference = DockerOperator(
    task_id='process_anthem_index_reference',
    image='anthem-streaming:latest',
    api_version='auto',
    auto_remove='success',  # In Airflow 3, this must be 'never', 'success', or 'force',
    docker_url="unix://var/run/docker.sock",  # For Docker
    command=[
        "--index-file", INDEX_FILE,
        "--warehouse-location", WAREHOUSE,
        "--catalog-name", CATALOG,
        "--file-refs-table", TABLE,
        "--batch-size", str(BATCH_SIZE),
        "--max-records", str(MAX_RECORDS),
        "--progress-interval", str(PROGRESS_INTERVAL)
    ],
    environment={
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_secret_key,
        'AWS_REGION': aws_region
    },
    dag=dag,
)
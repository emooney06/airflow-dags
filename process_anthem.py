from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'Ethan',
    'retries': 1,
}

# Define the DAG
@dag(
    default_args=default_args,
    schedule='@monthly',  # Schedule the DAG to run monthly
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['Anthem', 'Index'],
)
def process_anthem():

    @task
    def download_file():
        """
        Downloads a file from a URL and saves it to a local file.

        Args:
            None

        Returns:
            str: the path to the local file
        """
        url = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2025-05-01_anthem_index.json.gz'
        local_path = '/tmp/2025-05-01_anthem_index.json.gz'
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.raw.read())
            return local_path
        else:
            raise Exception(f"Failed to download file, status code: {response.status_code}")

    @task
    def upload_to_s3(local_path: str):
        """
        Uploads a local file to an S3 bucket.

        Args:
            local_path: str, the path to the local file to upload

        Returns:
            None
        """
        s3_bucket = 'price-transparency-raw'
        s3_key = 'payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz'
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(filename=local_path, key=s3_key, bucket_name=s3_bucket, replace=True)

    # Define task dependencies
    local_file_path = download_file()
    upload_to_s3(local_file_path)

# Instantiate the DAG
dag = process_anthem()
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import boto3
from io import BytesIO

# Default arguments for the DAG
default_args = {
    'owner': 'Ethan',
    'retries': 1,
}

@dag(
    default_args=default_args,
    schedule='@monthly',  # Schedule the DAG to run monthly
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['Anthem', 'Index'],
)
def process_anthem():

    @task
    def download_and_upload_file():
        """
        Downloads a file from a URL and uploads it directly to an S3 bucket.

        Args:
            None

        Returns:
            None
        """
        url = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2025-05-01_anthem_index.json.gz'
        s3_bucket = 'price-transparency-raw'
        s3_key = 'payer/anthem/index_files/main-index/2025-05-01_anthem_index.json.gz'
        
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            s3_client = boto3.client('s3')
            s3_client.upload_fileobj(BytesIO(response.content), s3_bucket, s3_key)
            print(f"✅ Uploaded to s3://{s3_bucket}/{s3_key}")
        else:
            raise Exception(f"Failed to download file, status code: {response.status_code}")

    download_and_upload_file()

dag = process_anthem()
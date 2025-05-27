from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def hello_world():
    print("Hello from GitHub DAG!")
    return "success"

dag = DAG(
    'test_github_dag',
    description='Test DAG from GitHub',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)

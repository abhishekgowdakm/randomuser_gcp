from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
import pandas as pd
import requests
from google.cloud import bigquery



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1
}

def random_user_data():
    response = requests.get('https://randomuser.me/api')
    if response.status_code ==200:
        data = response.json()
        user_info = data['results'][0]
        return user_info
        print(user_info)
        print(f"Name: {user_info['name']['first']} {user_info['name']['last']}")
        print(f"Email: {user_info['email']}")

def process_user_data(data):
        print(f"Processing user data: {data}")
        # Add your data processing logic here
        data_df = pd.json_normalize(data)
        print(data_df.head())
        print(data_df.columns)
        return data_df

def google_bigquery_upload(data):
    client = bigquery.Client()
    dataset_id = 'your_dataset_id'
    table_id = 'processed_user_data'
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_APPEND"
    job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
    job.result()
    
with DAG(dag_id='my_dag', default_args=default_args, schedule_interval='@daily') as dag:
    task1 = PythonOperator(task_id='random_user_data_task', python_callable=random_user_data)
    task2 = PythonOperator(task_id='process_user_data_task', python_callable=process_user_data, op_kwargs={'data': task1.output})
    #task3 = PythonOperator(task_id='google_bigquery_upload_task', python_callable=google_bigquery_upload, op_kwargs={'data': task2.output})
    task1 >> task2 
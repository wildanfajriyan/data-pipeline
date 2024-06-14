from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dotenv import load_dotenv
import os

load_dotenv(override=True)

dag = DAG(
    dag_id='spark_submit_etl',
    description='simple DAG for submit spark job',
    start_date=datetime(2024, 6, 6),
    schedule_interval='@daily',
    catchup=False
)

submit_job = SparkSubmitOperator(
    task_id="spark_job_submit",
    conn_id="sparkconn",
    application=os.getenv('SPARK_APP'),
    jars= os.getenv('JARS_PATH'),
    dag=dag
)

submit_job

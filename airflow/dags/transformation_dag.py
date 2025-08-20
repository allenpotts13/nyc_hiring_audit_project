from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from data_processors import (
    main
)

def _run_data_processing(**kwargs):
    kwargs['ti'].log.info(f"Running data processing tasks for {kwargs['dag_run'].run_id}")

    try:
         main()
    except Exception as e:
        kwargs['ti'].log.error(f"Data processing failed: {e}")
        raise

    kwargs['ti'].log.info(f"Data processing tasks completed successfully for {kwargs['dag_run'].run_id}")
    return True

with DAG(
    dag_id="data_transformation_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    description="DAG for data transformation tasks",
    tags=["data", "transformation"]
) as dag:
    run_data_transformation = PythonOperator(
        task_id="run_data_transformation",
        python_callable=_run_data_processing,
        provide_context=True,
    )

    run_data_transformation
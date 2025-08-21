from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
import sys
import os

PROJECT_ROOT_IN_AIRFLOW = '/opt/airflow'
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))

try:
    from src.data_acquisition import (
        main
    )
except ImportError as e:
    print(f"Error importing modules: {e}")
    raise

def _run_data_acquisition(**kwargs):
    kwargs['ti'].log.info(f"Running data acquisition tasks for {kwargs['dag_run'].run_id}")

    api_results = main()
    if not api_results:
        kwargs['ti'].log.error("API call failed, skipping subsequent tasks.")
        raise Exception("API call failed")
    
    kwargs['ti'].log.info(f"Data acquisition tasks completed successfully for {kwargs['dag_run'].run_id}")
    return True

with DAG(
    dag_id="data_acquisition_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    description="DAG for data acquisition tasks",
    tags=["data", "ingestion"]
) as dag:

    run_data_acquisition = PythonOperator(
        task_id="run_data_acquisition",
        python_callable=_run_data_acquisition,
        provide_context=True,
    )

    run_data_acquisition
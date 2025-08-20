from airflow  import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import subprocess
from dotenv import load_dotenv

DBT_PROJECT_DIR_IN_AIRFLOW = "/opt/airflow/nyc_hiring_dbt"
DBT_PROFILES_DIR_IN_AIRFLOW = "/opt/airflow/nyc_hiring_dbt"

load_dotenv()

def run_dbt_command(dbt_command: str, **kwargs):
    dbt_env = os.environ.copy()
    dbt_env['DBT_PROFILES_DIR'] = DBT_PROFILES_DIR_IN_AIRFLOW

    full_command = f"dbt {dbt_command}"

    process = subprocess.run(
        full_command,
        cwd=DBT_PROJECT_DIR_IN_AIRFLOW,
        env=dbt_env,
        shell=True,
        capture_output=True,
        text=True
    )

    if process.returncode != 0:
        print(f"Error running dbt command: {full_command}")
        print(f"stdout:\n{process.stdout}")
        print(f"stderr:\n{process.stderr}")
        raise Exception(f"DBT command failed with return code {process.returncode}")
    else:
        print(f"DBT command succeeded: {full_command}")
        print(f"stdout:\n{process.stdout}")
        print(f"stderr:\n{process.stderr}")

with DAG(
    dag_id='nyc_hiring_dbt_pipeline',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['nyc', 'hiring', 'dbt', 'silver', 'bronze'],
) as dag:
    run_dbt_bronze_models = PythonOperator(
        task_id='run_dbt_bronze_models',
        python_callable=run_dbt_command,
        op_kwargs={'dbt_command': 'run --profile nyc_hiring_dbt --select bronze'}
    )

    run_dbt_silver_models = PythonOperator(
        task_id='run_dbt_silver_models',
        python_callable=run_dbt_command,
        op_kwargs={'dbt_command': 'run --profile nyc_hiring_dbt --select silver'}
    )

    run_dbt_bronze_models >> run_dbt_silver_models
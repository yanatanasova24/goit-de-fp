from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

PROJECT_DIR = "/Users/mac/Documents/GoIt/DE/olympic_project"
PYTHON_BIN = f"{PROJECT_DIR}/.venv/bin/python"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='olympic_batch_datalake_dag',
    default_args=default_args,
    description='ETL Multi-hop Data Lake: Bronze -> Silver -> Gold',
    schedule=None,
    catchup=False,
    tags=['datalake', 'spark', 'olympics']
) as dag:

    landing_to_bronze_bio = BashOperator(
        task_id='landing_to_bronze_athlete_bio',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} landing_to_bronze.py athlete_bio"
    )

    landing_to_bronze_results = BashOperator(
        task_id='landing_to_bronze_athlete_event_results',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} landing_to_bronze.py athlete_event_results"
    )

    bronze_to_silver_bio = BashOperator(
        task_id='bronze_to_silver_athlete_bio',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} bronze_to_silver.py athlete_bio"
    )

    bronze_to_silver_results = BashOperator(
        task_id='bronze_to_silver_athlete_event_results',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} bronze_to_silver.py athlete_event_results"
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold_avg_stats',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} silver_to_gold.py"
    )

    landing_to_bronze_bio >> bronze_to_silver_bio
    landing_to_bronze_results >> bronze_to_silver_results
    [bronze_to_silver_bio, bronze_to_silver_results] >> silver_to_gold

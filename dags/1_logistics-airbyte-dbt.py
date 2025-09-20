from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'email': ['benjaminimp10@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG("1_logistics_airbyte_ingestion_dbt_silver",
         default_args=default_args,
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         tags=['silver', 'snowflake', 'logistics'],
         catchup=False) as dag:

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt/dbt-project && dbt test'
    )
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/dbt/dbt-project && dbt run --select path:models/silver/'
    )


    #check_orders = BashOperator(
    #task_id='check_delayed_orders',
    #bash_command='python /opt/airflow/dags/utils/check_delayed_orders.py'
    #)

    dbt_test >> dbt_run_silver 

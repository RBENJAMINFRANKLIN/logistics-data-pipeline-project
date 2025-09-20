from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from weather_util import fetch_weather_data_save_csv  
from utils.aws_utils import archive_and_clean_s3_folder
import requests




def print_date():
    print('Today is {}'.format(datetime.today().date()))


def call_weather_api():
    fetch_weather_data_save_csv()


BUCKET = 'snowflake-june2025'
DATA_PREFIX = 'weather_data/unprocessed/'  # only contents inside this
ARCHIVE_BASE_PREFIX = 'weather_data/archive/'  # timestamped subfolder will be created here

def run_archive():
    archive_and_clean_s3_folder(BUCKET, DATA_PREFIX, ARCHIVE_BASE_PREFIX)


dag = DAG(
    'weather_data_dag',
    default_args={'start_date': days_ago(1)},
)




print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag

)


weather_task = PythonOperator(
    task_id='call_weather_rest_api',
    python_callable=call_weather_api,
    dag=dag
)


archive_task = PythonOperator(
        task_id='archive_s3_files',
        python_callable=run_archive
    )

    # Task 4: Insert into dim_customer
copy_command_snowflake_task = SnowflakeOperator(
        task_id='copy_command_snowflake',
        snowflake_conn_id='snowflakeid',retries=0,
        sql="""  
            COPY INTO demo.demoschema.weather_data
            FROM @demo.demoschema.aws_demo_stage2/unprocessed/
            FILE_FORMAT = (FORMAT_NAME = demo.demoschema.weather_csv_format)
            PATTERN = '.*weather_.*\.csv';
       """,
    )

# Set the dependencies between the tasks

weather_task >> copy_command_snowflake_task >> archive_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello():
    print("Hello, Airflow!")


def print_hello2():
    print(" Welcome     to ariflow  !")     


with DAG(   dag_id='demo_dag',
    start_date=datetime(2025,6, 1),
    #schedule_interval='0 10 * * *',  # Runs daily at 10:00 AM
    #schedule_interval='0 10,14,18 * * *',
    schedule_interval="@daily",  # Runs daily
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,
    )   

    hello_task2 = PythonOperator(
        task_id='print_hello2_task',
        python_callable=print_hello2,
    )

    hello_task >> hello_task2   
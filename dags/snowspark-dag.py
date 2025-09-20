from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from snowflake.snowpark import Session
import yaml
from snowflake.snowpark.functions import row_number,lit


def load_snowflake_config():
    with open('dags/utils/snowflake_config.yaml') as f:
        config = yaml.safe_load(f)
    return config['snowflake']

def run_snowpark_logic():
    connection_parameters = load_snowflake_config()
    session = Session.builder.configs(connection_parameters).create()
    # Save to new table
    df1 = session.table("DEMO2.RAW.SALES_TRANSACTIONS")
    df1.show()


    df2 = df1.drop("CATEGORY").drop("REGION")
    df2 = df2.with_column("country",lit("USA"))

    df2.show()

    df_deduped = df2.drop_duplicates(["CUSTOMER_ID", "ORDER_ID"])
    df_deduped.show()

    df_deduped.write.mode("append").save_as_table("SALES_TRANSACTIONS_DEDUPED")

with DAG("snowpark_demo1_dag", 
         start_date=datetime(2023, 1, 1), 
         schedule_interval=None, 
         catchup=False) as dag:
    
    snowpark_task = PythonOperator(
        task_id="run_snowpark",
        python_callable=run_snowpark_logic
    )

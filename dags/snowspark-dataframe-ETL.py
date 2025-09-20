
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.aws_utils import archive_and_clean_s3_folder, send_failure_sns_alert
from utils.check_files import skip_if_no_files
from snowflake.snowpark import Session, functions as F
from snowflake.snowpark.types import (
    StringType,
    IntegerType,
    FloatType,DateType)

import yaml
def load_snowflake_config():
    with open('dags/utils/snowflake_config.yaml') as f:
        config = yaml.safe_load(f)
    return config['snowflake']


def get_snowflake_session():
    connection_parameters = load_snowflake_config()
    return Session.builder.configs(connection_parameters).create()

def process_bronze_stage(session):
    df_raw = session.read.format("csv").options({
    "field_delimiter": ",",
    "skip_header": 1,
    "field_optionally_enclosed_by": '"',
    "null_if": "None,NULL,",  # Comma-separated values
    "trim_space": True
    }).load("@EDW.SALES_BRONZE.MY_AWS_STAGE/supply_sales/unprocessed/")
    df_raw.show()
    print(df_raw.count())
    df_raw.print_schema()
    
    df_casted = df_raw.select(
        df_raw.col('"c1"').cast(StringType()).alias("SALES_ORDER_NUMBER"),
        df_raw.col('"c2"').cast(IntegerType()).alias("SALES_ORDER_LINENUMBER"),
        df_raw.col('"c3"').cast(DateType()).alias("ORDER_DATE"),
        df_raw.col('"c4"').alias("CUSTOMER_NAME"),
        df_raw.col('"c5"').alias("EMAIL"),
        df_raw.col('"c6"').alias("ITEM"),
        df_raw.col('"c7"').cast(IntegerType()).alias("QUANTITY"),
        df_raw.col('"c8"').cast(FloatType()).alias("UNITPRICE"),
        df_raw.col('"c9"').cast(FloatType()).alias("TAX"),
    )

    df_valid = df_casted.filter(
        (F.col("SALES_ORDER_NUMBER").is_not_null()) &
        (F.col("SALES_ORDER_LINENUMBER").is_not_null()) &
        (F.col("ORDER_DATE").is_not_null()) &
        (F.col("QUANTITY") > 0) &
        (F.col("UNITPRICE") >= 0) &
        (F.col("TAX") > 0)
    )



    df_valid.write.mode("overwrite").save_as_table("EDW.SALES_BRONZE.SALES_BRONZE_VALID_TEMP")

    from snowflake.snowpark.functions import col, when, lit

    df_error = df_raw.with_column(
        "ERROR_REASON",
        when(col('"c1"').cast(StringType()).is_null(), lit("Invalid SALES_ORDER_NUMBER"))
        .when(col('"c2"').cast(IntegerType()).is_null(), lit("Invalid SALES_ORDER_LINENUMBER"))
        .when(col('"c3"').cast(DateType()).is_null(), lit("Invalid ORDER_DATE"))
        .when(col('"c7"').cast(IntegerType()).is_null(), lit("Invalid QUANTITY"))
        .when(col('"c8"').cast(FloatType()).is_null(), lit("Invalid UNITPRICE"))
        .when(col('"c9"').cast(FloatType()).is_null(), lit("Invalid TAX"))
        .otherwise(lit("Unknown Error"))
    )


    df_error.write.mode("overwrite").save_as_table("EDW.SALES_BRONZE.SALES_BRONZE_ERROR_TEMP")
from snowflake.snowpark.functions import col, current_timestamp

def process_sales_silver(session):
    df_valid = session.table("EDW.SALES_BRONZE.SALES_BRONZE_VALID_TEMP")

    # Filter and enrich
    df_valid_silver = df_valid.filter(
        (col("EMAIL").like("%@%")) &
        (col("CUSTOMER_NAME").is_not_null()) &
        (col("ITEM").is_not_null())
    ).with_column("CREATED_TS", current_timestamp())\
     .with_column("MODIFIED_TS", current_timestamp())

    target_table = session.table("EDW.SALES_SILVER.PROCESSED_SALES_DATA")

    # Perform the MERGE with correct Snowpark clause strings
    target_table.merge(
        source=df_valid_silver,
        join_expr=(
            (col("target.SALES_ORDER_NUMBER") == col("source.SALES_ORDER_NUMBER")) &
            (col("target.ORDER_DATE") == col("source.ORDER_DATE")) &
            (col("target.EMAIL") == col("source.EMAIL")) &
            (col("target.ITEM") == col("source.ITEM"))
        ),
        clauses=[
            # WHEN MATCHED: UPDATE
            (
                "WHEN MATCHED THEN UPDATE SET "
                "SALES_ORDER_LINENUMBER = source.SALES_ORDER_LINENUMBER, "
                "EMAIL = source.EMAIL, "
                "QUANTITY = source.QUANTITY, "
                "UNITPRICE = source.UNITPRICE, "
                "TAX = source.TAX, "
                "MODIFIED_TS = source.MODIFIED_TS"
            ),
            # WHEN NOT MATCHED: INSERT
            (
                "WHEN NOT MATCHED THEN INSERT ("
                "SALES_ORDER_NUMBER, SALES_ORDER_LINENUMBER, ORDER_DATE, CUSTOMER_NAME, "
                "EMAIL, ITEM, QUANTITY, UNITPRICE, TAX, CREATED_TS, MODIFIED_TS"
                ") VALUES ("
                "source.SALES_ORDER_NUMBER, source.SALES_ORDER_LINENUMBER, source.ORDER_DATE, source.CUSTOMER_NAME, "
                "source.EMAIL, source.ITEM, source.QUANTITY, source.UNITPRICE, source.TAX, source.CREATED_TS, source.MODIFIED_TS"
                ")"
            )
        ]
    )


def process_dim_tables(session):
    df_proc = session.table("EDW.SALES_SILVER.PROCESSED_SALES_DATA")

    df_dim_date = df_proc.select(
        F.col("ORDER_DATE"),
        F.replace(F.to_char("ORDER_DATE", "YYYYMMDD"), "-", "").alias("ORDER_DATE_ID"),
        F.year("ORDER_DATE").alias("YEAR"),
        F.month("ORDER_DATE").alias("MONTH"),
        F.dayofmonth("ORDER_DATE").alias("DAY"),
        F.substring(F.to_char("ORDER_DATE", "YYYYMM"), 1, 6).alias("YYYYMM")
    ).distinct()



    df_dim_customer = df_proc.group_by("EMAIL").agg(
        F.max("CUSTOMER_NAME").alias("CUSTOMER_NAME"),
        F.when(F.count("SALES_ORDER_NUMBER") == 1, F.lit("New Customer"))
         .when(F.count("SALES_ORDER_NUMBER") > 1, F.lit("Returning Customer"))
         .otherwise(F.lit("Regular Customer")).alias("CUSTOMER_TYPE")
    )

    from snowflake.snowpark.functions import col
    target_table = session.table("EDW.SALES_SILVER.DIM_DATE")
    source_df = df_dim_date

    # Run merge directly on the target table
    target_table.merge(
        source=source_df,
        join_expr=col("target.ORDER_DATE_ID") == col("source.ORDER_DATE_ID"),
        clauses=[
            ("WHEN MATCHED THEN UPDATE SET ORDER_DATE = source.ORDER_DATE"),
            (
                "WHEN NOT MATCHED THEN INSERT (ORDER_DATE_ID, ORDER_DATE, YEAR, MONTH, DAY, YYYYMM) "
                "VALUES (source.ORDER_DATE_ID, source.ORDER_DATE, source.YEAR, source.MONTH, source.DAY, source.YYYYMM)"
            )
        ]
    )

    df_dim_product = df_proc.select("ITEM").distinct().with_column(
        "CATEGORY", F.when(F.col("ITEM").ilike("%Laptop%"), F.lit("Electronics"))
            .when(F.col("ITEM").ilike("%Phone%"), F.lit("Mobile Devices"))
            .when(F.col("ITEM").ilike("%Shoes%"), F.lit("Footwear"))
            .when(F.col("ITEM").ilike("%T-shirt%"), F.lit("Clothing"))
            .otherwise(F.lit("Others"))
    ).with_column_renamed("ITEM", "ITEM_NAME")

    session.table("EDW.SALES_SILVER.DIM_PRODUCT").merge(
        source=df_dim_product,
        condition=F.col("target.ITEM_NAME") == F.col("source.ITEM_NAME"),
        when_matched_update={"CATEGORY": F.col("source.CATEGORY")},
        when_not_matched_insert={
            "ITEM_NAME": F.col("source.ITEM_NAME"),
            "CATEGORY": F.col("source.CATEGORY")
        }
    )

def process_fact_sales(session):
    df = session.table("EDW.SALES_SILVER.PROCESSED_SALES_DATA")
    df_customer = session.table("EDW.SALES_SILVER.DIM_CUSTOMER")
    df_product = session.table("EDW.SALES_SILVER.DIM_PRODUCT")
    df_date = session.table("EDW.SALES_SILVER.DIM_DATE")

    df_fact = df.join(df_customer, df["EMAIL"] == df_customer["EMAIL"], how="left")\
        .join(df_product, df["ITEM"] == df_product["ITEM_NAME"], how="left")\
        .join(df_date, df["ORDER_DATE"] == df_date["ORDER_DATE"], how="left")\
        .select(
            df["SALES_ORDER_NUMBER"],
            df_customer["CUSTOMER_ID"].cast(IntegerType()).alias("CUSTOMER_ID"),
            df_product["ITEM_ID"].cast(IntegerType()).alias("ITEM_ID"),
            df_date["ORDER_DATE_ID"].cast(IntegerType()).alias("ORDER_DATE_ID"),
            df["QUANTITY"],
            df["UNITPRICE"].alias("UNIT_PRICE"),
            df["TAX"],
            (df["QUANTITY"] * df["UNITPRICE"] + df["TAX"]).alias("TOTAL_SALES_AMOUNT")
        )

    session.table("EDW.SALES_SILVER.FACT_SALES").merge(
        source=df_fact,
        condition=(F.col("target.SALES_ORDER_NUMBER") == F.col("source.SALES_ORDER_NUMBER")) &
                  (F.col("target.CUSTOMER_ID") == F.col("source.CUSTOMER_ID")) &
                  (F.col("target.ORDER_DATE_ID") == F.col("source.ORDER_DATE_ID")) &
                  (F.col("target.ITEM_ID") == F.col("source.ITEM_ID")),
        when_matched_update={
            "QUANTITY": F.col("source.QUANTITY"),
            "UNIT_PRICE": F.col("source.UNIT_PRICE"),
            "TAX": F.col("source.TAX"),
            "TOTAL_SALES_AMOUNT": F.col("source.TOTAL_SALES_AMOUNT")
        },
        when_not_matched_insert={
            "SALES_ORDER_NUMBER": F.col("source.SALES_ORDER_NUMBER"),
            "CUSTOMER_ID": F.col("source.CUSTOMER_ID"),
            "ITEM_ID": F.col("source.ITEM_ID"),
            "ORDER_DATE_ID": F.col("source.ORDER_DATE_ID"),
            "QUANTITY": F.col("source.QUANTITY"),
            "UNIT_PRICE": F.col("source.UNIT_PRICE"),
            "TAX": F.col("source.TAX"),
            "TOTAL_SALES_AMOUNT": F.col("source.TOTAL_SALES_AMOUNT")
        }
    )

def process_gold_aggregations(session):
    fact = session.table("EDW.SALES_SILVER.FACT_SALES")
    dim_customer = session.table("EDW.SALES_SILVER.DIM_CUSTOMER")
    dim_date = session.table("EDW.SALES_SILVER.DIM_DATE")
    dim_product = session.table("EDW.SALES_SILVER.DIM_PRODUCT")

    # Customer sales agg
    fact.join(dim_customer, "CUSTOMER_ID").join(dim_date, "ORDER_DATE_ID")\
        .group_by("CUSTOMER_ID", "CUSTOMER_NAME", "YEAR", "MONTH", "YYYYMM")\
        .agg(
            F.sum("QUANTITY").alias("TOTAL_QUANTITY"),
            F.sum("TOTAL_SALES_AMOUNT").alias("TOTAL_REVENUE"),
            F.count_distinct("ORDER_DATE").alias("ACTIVE_DAYS")
        )\
        .write.mode("overwrite").save_as_table("EDW.SALES_GOLD.FACT_CUSTOMER_SALES_AGG")

    # Sales summary by day
    fact.join(dim_date, "ORDER_DATE_ID")\
        .group_by("ORDER_DATE")\
        .agg(
            F.sum("TOTAL_SALES_AMOUNT").alias("TOTAL_SALES"),
            F.sum("QUANTITY").alias("TOTAL_UNITS_SOLD"),
            F.count_distinct("SALES_ORDER_NUMBER").alias("TOTAL_ORDERS")
        )\
        .write.mode("overwrite").save_as_table("EDW.SALES_GOLD.SALES_SUMMARY_BY_DAY")

    # Product sales summary
    fact.join(dim_product, "ITEM_ID")\
        .group_by("ITEM_ID", "ITEM_NAME", "CATEGORY")\
        .agg(
            F.sum("QUANTITY").alias("UNITS_SOLD"),
            F.sum("TOTAL_SALES_AMOUNT").alias("REVENUE"),
            F.count_distinct("SALES_ORDER_NUMBER").alias("ORDER_COUNT")
        )\
        .write.mode("overwrite").save_as_table("EDW.SALES_GOLD.PRODUCT_SALES_SUMMARY")


def call_bronze_stage(**kwargs):
    session = get_snowflake_session()
    process_bronze_stage(session)
    session.close()

def call_sales_silver(**kwargs):
    session = get_snowflake_session()
    process_sales_silver(session)
    session.close()

def call_dim_tables(**kwargs):
    session = get_snowflake_session()
    process_dim_tables(session)
    session.close()

def call_fact_sales(**kwargs):
    session = get_snowflake_session()
    process_fact_sales(session)
    session.close()

def call_gold_aggregations(**kwargs):
    session = get_snowflake_session()
    process_gold_aggregations(session)
    session.close()
def run_archive():
    archive_and_clean_s3_folder(
        bucket='mar2025-training-bucket',
        prefix='EDW/supply_sales/unprocessed/',
        archive_prefix='EDW/supply_sales/archive/'
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': send_failure_sns_alert,
}

with DAG("snowpark_dataframe_dag_1", 
         start_date=datetime(2023, 1, 1), 
         schedule_interval=None, 
         catchup=False) as dag:
    check_files = PythonOperator(
        task_id='check_stage_files',
        python_callable=skip_if_no_files
    )

    bronze_task = PythonOperator(
        task_id='call_bronze_stage',
        python_callable=call_bronze_stage,
        provide_context=True
    )

    silver_task = PythonOperator(
        task_id='call_sales_silver',
        python_callable=call_sales_silver,
        provide_context=True
    )

    dim_tables_task = PythonOperator(
        task_id='call_dim_tables',
        python_callable=call_dim_tables,
        provide_context=True
    )

    fact_sales_task = PythonOperator(
        task_id='call_fact_sales',
        python_callable=call_fact_sales,
        provide_context=True
    )

    gold_agg_task = PythonOperator(
        task_id='call_gold_aggregations',
        python_callable=call_gold_aggregations,
        provide_context=True
    )

    archive_task = PythonOperator(
        task_id='archive_s3_files',
        python_callable=run_archive
    )
    check_files >> bronze_task >> silver_task >> dim_tables_task >> fact_sales_task >> gold_agg_task >> archive_task

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
default_args = {
    'owner': 'data_eng',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='3_logistics_gold_metrics_dbt',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'snowflake', 'logistics'],
    description='Generates analytical summary views from silver layer',
) as dag:

    shipment_delay_analysis = SnowflakeOperator(
        task_id='shipment_delay_analysis',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.shipment_delay_summary AS
            SELECT
                f.CARRIER_NAME AS courier_name,
                f.DESTINATION_CITY AS delivery_zone,
                COUNT(*) AS total_shipments,
                AVG(DATEDIFF(day, f.STATUS_CREATED_AT, f.STATUS_DELIVERED_AT)) AS avg_delivery_days,
                SUM(CASE WHEN f.DELAY_FLAG THEN 1 ELSE 0 END) AS delayed_shipments,
                ROUND(100.0 * SUM(CASE WHEN f.DELAY_FLAG THEN 1 ELSE 0 END) / COUNT(*), 2) AS delay_rate_pct
            FROM LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS f
            WHERE f.STATUS_CREATED_AT >= CURRENT_DATE - 30
            GROUP BY f.CARRIER_NAME, f.DESTINATION_CITY
            ORDER BY delay_rate_pct DESC;
        """
    )

    seller_rto_performance = SnowflakeOperator(
        task_id='seller_rto_performance',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.seller_rto_performance AS
        SELECT
            s.seller_id,
            s.seller_name,
            COUNT(*) AS total_orders,
            ROUND(AVG(f.delivery_tat_days), 2) AS avg_tat,
            SUM(CASE WHEN f.rto_flag THEN 1 ELSE 0 END) AS rto_orders,
            ROUND(100.0 * SUM(CASE WHEN f.rto_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS rto_pct,
            ROUND(SUM(f.shipping_cost), 2) AS total_shipping_cost
        FROM LOGISTICS_DEMO_1.silver.fact_shipments f
        JOIN LOGISTICS_DEMO_1.silver.dim_sellers s ON f.seller_id = s.seller_id
        GROUP BY s.seller_id, s.seller_name
        ORDER BY rto_pct DESC;
        """
    )

    dbt_run_test = BashOperator(
        task_id='dbt_run_test',
        bash_command='cd /opt/airflow/dbt/dbt-project && dbt test'
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt/dbt-project && dbt run --select path:models/gold/'
    )

    # Task Dependency chain (linear or modify as per execution plan)
    (
        shipment_delay_analysis 
        >> seller_rto_performance
        >> dbt_run_test
        >> dbt_run_gold
    )

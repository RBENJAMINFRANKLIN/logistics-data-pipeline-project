from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_eng',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='logistics_gold_metrics_sql',
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
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.seller_performance AS
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

    courier_sla_breach = SnowflakeOperator(
        task_id='courier_sla_breach',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.courier_sla_breach AS
        SELECT
            c.name AS courier_name,
            COUNT(*) AS total_shipments,
            SUM(CASE WHEN f.delay_flag THEN 1 ELSE 0 END) AS sla_breaches,
            ROUND(100.0 * SUM(CASE WHEN f.delay_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS breach_pct
        FROM LOGISTICS_DEMO_1.silver.fact_shipments f
        JOIN LOGISTICS_DEMO_1.silver.dim_couriers c ON f.CARRIER_ID = c.courier_id
        GROUP BY c.name
        ORDER BY breach_pct DESC;
        """
    )

    delivery_performance_analysis = SnowflakeOperator(
        task_id='delivery_performance_analysis',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.delivery_performance_summary AS
        SELECT 
            s.CARRIER_NAME,
            COUNT(*) AS total_shipments,
            AVG(s.DELIVERY_TAT_DAYS) AS avg_delivery_days,
            SUM(CASE WHEN s.DELAY_FLAG THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS delay_percentage
        FROM LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS s
        JOIN LOGISTICS_DEMO_1.SILVER.DIM_COURIERS c ON s.CARRIER_ID = c.COURIER_ID
        GROUP BY s.CARRIER_NAME;
        """
    )

    inventory_vs_orders = SnowflakeOperator(
        task_id='inventory_vs_orders',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.inventory_order_summary AS
        SELECT 
            i.PRODUCT_ID,
            p.PRODUCT_NAME,
            i.STOCK,
            COUNT(o.ORDER_ID) AS orders_last_30_days
        FROM LOGISTICS_DEMO_1.SILVER.FACT_INVENTORY i
        LEFT JOIN LOGISTICS_DEMO_1.SILVER.FACT_ORDERS o 
            ON i.PRODUCT_ID = o.PRODUCT_ID 
            AND o.ORDER_DATE >= DATEADD(DAY, -30, CURRENT_DATE)
        JOIN LOGISTICS_DEMO_1.SILVER.DIM_PRODUCTS p ON i.PRODUCT_ID = p.PRODUCT_ID
        GROUP BY i.PRODUCT_ID, p.PRODUCT_NAME, i.STOCK;
        """
    )

    shipment_cost_analysis = SnowflakeOperator(
        task_id='shipment_cost_analysis',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.shipment_cost_summary AS
        SELECT 
            s.CARRIER_NAME,
            ROUND(AVG(SHIPPING_COST), 2) AS avg_shipping_cost,
            ROUND(AVG(FUEL_SURCHARGE), 2) AS avg_fuel_surcharge,
            ROUND(AVG(INSURANCE), 2) AS avg_insurance,
            ROUND(AVG(COD_FEE), 2) AS avg_cod_fee
        FROM LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS s
        GROUP BY s.CARRIER_NAME;
        """
    )

    order_to_delivery_lifecycle = SnowflakeOperator(
        task_id='order_to_delivery_lifecycle',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.order_delivery_lifecycle AS
        SELECT 
            o.ORDER_ID,
            o.ORDER_DATE,
            s.CREATED_AT AS shipment_created_at,
            s.DELIVERED_AT,
            DATEDIFF(DAY, o.ORDER_DATE, s.CREATED_AT) AS order_to_shipment_days,
            DATEDIFF(DAY, s.CREATED_AT, s.DELIVERED_AT) AS shipment_to_delivery_days,
            DATEDIFF(DAY, o.ORDER_DATE, s.DELIVERED_AT) AS total_fulfillment_days
        FROM LOGISTICS_DEMO_1.SILVER.FACT_ORDERS o
        JOIN LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS s ON o.ORDER_ID = s.ORDER_ID 
        WHERE s.status = 'Delivered';
        """
    )

    seller_summary = SnowflakeOperator(
        task_id='seller_summary',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.seller_summary AS
        SELECT 
            s.SELLER_ID,
            sel.SELLER_NAME,
            COUNT(DISTINCT o.ORDER_ID) AS total_orders,
            COUNT(DISTINCT shp.SHIPMENT_ID) AS total_shipments,
            AVG(shp.DELIVERY_TAT_DAYS) AS avg_delivery_time,
            SUM(CASE WHEN shp.RTO_FLAG THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS rto_percentage
        FROM LOGISTICS_DEMO_1.SILVER.FACT_ORDERS o
        JOIN LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS shp ON o.ORDER_ID = shp.ORDER_ID
        JOIN LOGISTICS_DEMO_1.SILVER.FACT_INVENTORY s ON o.PRODUCT_ID = s.PRODUCT_ID
        JOIN LOGISTICS_DEMO_1.SILVER.DIM_SELLERS sel ON s.SELLER_ID = sel.SELLER_ID
        GROUP BY s.SELLER_ID, sel.SELLER_NAME;
        """
    )

    customer_experience = SnowflakeOperator(
        task_id='customer_experience',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.customer_experience_summary AS
        SELECT 
            cust.CUSTOMER_ID,
            COUNT(DISTINCT o.ORDER_ID) AS orders_count,
            SUM(CASE WHEN shp.DELAY_FLAG THEN 1 ELSE 0 END) AS delayed_shipments,
            ROUND(AVG(DATEDIFF(DAY, o.ORDER_DATE, shp.DELIVERED_AT)), 2) AS avg_delivery_time
        FROM LOGISTICS_DEMO_1.SILVER.FACT_ORDERS o
        JOIN LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS shp ON o.ORDER_ID = shp.ORDER_ID
        JOIN LOGISTICS_DEMO_1.SILVER.DIM_CUSTOMERS cust ON o.CUSTOMER_ID = cust.CUSTOMER_ID
        GROUP BY cust.CUSTOMER_ID;
        """
    )

    geo_delivery_dashboard = SnowflakeOperator(
        task_id='geo_delivery_dashboard',
        snowflake_conn_id='snowflakeid',
        sql="""
        CREATE OR REPLACE VIEW LOGISTICS_DEMO_1.gold.geo_delivery_summary AS
        SELECT 
            s.DESTINATION_PINCODE,
            l.CITY,
            COUNT(*) AS shipment_count,
            ROUND(AVG(s.DELIVERY_TAT_DAYS), 2) AS avg_delivery_days,
            SUM(CASE WHEN s.DELAY_FLAG THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS delay_rate
        FROM LOGISTICS_DEMO_1.SILVER.FACT_SHIPMENTS s
        JOIN LOGISTICS_DEMO_1.SILVER.DIM_LOCATIONS l ON s.DESTINATION_PINCODE = l.PINCODE
        GROUP BY s.DESTINATION_PINCODE, l.CITY;
        """
    )

    # Task Dependency chain (linear or modify as per execution plan)
    (
        shipment_delay_analysis 
        >> seller_rto_performance 
        >> courier_sla_breach 
        >> delivery_performance_analysis 
        >> inventory_vs_orders 
        >> shipment_cost_analysis 
        >> order_to_delivery_lifecycle 
        >> seller_summary 
        >> customer_experience 
        >> geo_delivery_dashboard
    )

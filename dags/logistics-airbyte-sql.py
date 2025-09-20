from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2025, 6, 24),
    'retries': 0
}

with DAG(
    dag_id='logistics_airbyte_ingestion_sql_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['airbyte', 'snowflake', 'logistics'],
) as dag:

    # Task 1: Upsert dim_customers
    upsert_dim_customers = SnowflakeOperator(
        task_id='upsert_dim_customers',
        snowflake_conn_id='snowflakeid',
        sql="""


                MERGE INTO LOGISTICS_DEMO_1.SILVER.dim_customers AS tgt
                USING (
                    SELECT 
                        CUSTOMER_ID AS customer_id,
                        NAME AS name,
                        EMAIL AS email,
                        REGISTERED_AT AS created_at,
                        _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                        _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at  -- 🔁 FIXED HERE
                    FROM airbyte_database.logistics.customers
                    WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                        (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.SILVER.dim_customers), 
                        '2000-01-01'::TIMESTAMP_NTZ
                    )
                ) AS src
                ON tgt.customer_id = src.customer_id
                WHEN MATCHED THEN UPDATE SET
                    name = src.name,
                    email = src.email,
                    updated_at = src.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    customer_id, name, email, created_at, updated_at, _airbyte_emitted_at
                ) VALUES (
                    src.customer_id, src.name, src.email, src.created_at, src.updated_at, src._airbyte_emitted_at
                );

        """
    )


     # Task 2: Upsert dim_couriers
    upsert_dim_couriers = SnowflakeOperator(
        task_id='upsert_dim_couriers',
        snowflake_conn_id='snowflakeid',
        sql="""
            MERGE INTO LOGISTICS_DEMO_1.SILVER.dim_couriers AS tgt
            USING (
                SELECT 
                    COURIER_ID AS courier_id,
                    NAME AS name,
                    SUPPORT_CONTACT AS support_contact,
                    _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                    _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at
                FROM airbyte_database.logistics.couriers
                WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                    (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.SILVER.dim_couriers), 
                    '2000-01-01'::TIMESTAMP_NTZ
                )
            ) AS src
            ON tgt.courier_id = src.courier_id
            WHEN MATCHED THEN UPDATE SET
                name = src.name,
                support_contact = src.support_contact,
                updated_at = src.updated_at
            WHEN NOT MATCHED THEN INSERT (
                courier_id, name, support_contact, updated_at, _airbyte_emitted_at
            ) VALUES (
                src.courier_id, src.name, src.support_contact, src.updated_at, src._airbyte_emitted_at
            );

        """
    )

    # Task 3: Upsert dim_locations
    upsert_dim_locations = SnowflakeOperator(
        task_id='upsert_dim_locations',
        snowflake_conn_id='snowflakeid',
        sql="""
                MERGE INTO LOGISTICS_DEMO_1.SILVER.dim_locations AS tgt
                USING (
                    SELECT 
                        LOCATION_ID AS location_id,
                        CITY AS city,
                        ZONE AS zone,
                        STATE AS state,
                        PINCODE AS pincode,
                        _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                        _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at
                    FROM airbyte_database.logistics.locations
                    WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                        (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.SILVER.dim_locations), 
                        '2000-01-01'::TIMESTAMP_NTZ
                    )
                ) AS src
                ON tgt.location_id = src.location_id
                WHEN MATCHED THEN UPDATE SET
                    city = src.city,
                    zone = src.zone,
                    state = src.state,
                    pincode = src.pincode,
                    updated_at = src.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    location_id, city, zone, state, pincode, updated_at, _airbyte_emitted_at
                ) VALUES (
                    src.location_id, src.city, src.zone, src.state, src.pincode, src.updated_at, src._airbyte_emitted_at
                );

        """
    )

    # Task 4: Upsert dim_products
    upsert_dim_products = SnowflakeOperator(
        task_id='upsert_dim_products',
        snowflake_conn_id='snowflakeid',
        sql="""
                MERGE INTO LOGISTICS_DEMO_1.SILVER.dim_products AS tgt
                USING (
                    SELECT 
                        PRODUCT_ID AS product_id,
                        PRODUCT_NAME AS product_name,
                        CATEGORY AS category,
                        PRICE AS price,
                        _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                        _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at
                    FROM airbyte_database.logistics.products
                    WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                        (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.SILVER.dim_products), 
                        '2000-01-01'::TIMESTAMP_NTZ
                    )
                ) AS src
                ON tgt.product_id = src.product_id
                WHEN MATCHED THEN UPDATE SET
                    product_name = src.product_name,
                    category = src.category,
                    price = src.price,
                    updated_at = src.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    product_id, product_name, category, price, updated_at, _airbyte_emitted_at
                ) VALUES (
                    src.product_id, src.product_name, src.category, src.price, src.updated_at, src._airbyte_emitted_at
                );

        """
    )

    # Task 5: Upsert dim_sellers
    upsert_dim_sellers = SnowflakeOperator(
        task_id='upsert_dim_sellers',
        snowflake_conn_id='snowflakeid',
        sql="""
           
            MERGE INTO LOGISTICS_DEMO_1.SILVER.dim_sellers AS tgt
            USING (
                SELECT 
                    SELLER_ID AS seller_id,
                    SELLER_NAME AS seller_name,
                    GST_NUMBER AS gst_number,
                    WAREHOUSE_LOCATION AS warehouse_location,
                    _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                    _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at
                FROM airbyte_database.logistics.sellers
                WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                    (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.SILVER.dim_sellers), 
                    '2000-01-01'::TIMESTAMP_NTZ
                )
            ) AS src
            ON tgt.seller_id = src.seller_id
            WHEN MATCHED THEN UPDATE SET
                seller_name = src.seller_name,
                gst_number = src.gst_number,
                warehouse_location = src.warehouse_location,
                updated_at = src.updated_at
            WHEN NOT MATCHED THEN INSERT (
                seller_id, seller_name, gst_number, warehouse_location, updated_at, _airbyte_emitted_at
            ) VALUES (
                src.seller_id, src.seller_name, src.gst_number, src.warehouse_location, src.updated_at, src._airbyte_emitted_at
            );


        """
    )

 
    upsert_fact_order = SnowflakeOperator(
        task_id='upsert_fact_order',
        snowflake_conn_id='snowflakeid',
        sql="""
        MERGE INTO LOGISTICS_DEMO_1.silver.fact_orders AS tgt
        USING (
            SELECT 
                ORDER_ID AS order_id,
                ORDER_DATE AS order_date,
                CUSTOMER_ID AS customer_id,
                PRODUCT_ID AS product_id,
                TOTAL_AMOUNT AS total_amount,
                PAYMENT_METHOD AS payment_method,
                _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at
            FROM airbyte_database.logistics.orders
            WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.silver.fact_orders),
                '1970-01-01'::TIMESTAMP_NTZ
            )
        ) AS src
        ON tgt.order_id = src.order_id
        WHEN MATCHED THEN UPDATE SET
            order_date = src.order_date,
            customer_id = src.customer_id,
            product_id = src.product_id,
            total_amount = src.total_amount,
            payment_method = src.payment_method,
            updated_at = src.updated_at
        WHEN NOT MATCHED THEN INSERT (
            order_id, order_date, customer_id, product_id, total_amount, payment_method, updated_at, _airbyte_emitted_at
        ) VALUES (
            src.order_id, src.order_date, src.customer_id, src.product_id, src.total_amount, src.payment_method, src.updated_at, src._airbyte_emitted_at
        );
    """)

    upsert_fact_inventory = SnowflakeOperator(
        task_id='upsert_fact_inventory',
        snowflake_conn_id='snowflakeid',
        sql=""" 
            MERGE INTO LOGISTICS_DEMO_1.SILVER.fact_inventory AS tgt
        USING (
            SELECT 
                ID AS id,
                SELLER_ID AS seller_id,
                PRODUCT_ID AS product_id,
                STOCK AS stock,
                LAST_UPDATED AS last_updated,
                _AB_CDC_UPDATED_AT::TIMESTAMP AS updated_at,
                _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _airbyte_emitted_at
            FROM airbyte_database.logistics.inventory
            WHERE _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ > COALESCE(
                (SELECT MAX(_airbyte_emitted_at) FROM LOGISTICS_DEMO_1.SILVER.fact_inventory),
                '1970-01-01'::TIMESTAMP_NTZ
            )
        ) AS src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            seller_id = src.seller_id,
            product_id = src.product_id,
            stock = src.stock,
            last_updated = src.last_updated,
            updated_at = src.updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, seller_id, product_id, stock, last_updated, updated_at, _airbyte_emitted_at
        ) VALUES (
            src.id, src.seller_id, src.product_id, src.stock, src.last_updated, src.updated_at, src._airbyte_emitted_at
        );
    """)


    # Optional: Set execution order (run in parallel or sequential)
[upsert_dim_customers, upsert_dim_couriers, upsert_dim_locations, upsert_dim_products, upsert_dim_sellers] >> upsert_fact_order  >> upsert_fact_inventory


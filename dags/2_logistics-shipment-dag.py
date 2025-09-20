from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from snowflake.email_utils import query_snowflake_and_send_report
import requests
import boto3
import os
import json

# Constants
API_URL = "http://104.237.2.219:9050/generate-shipments?100"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"  # Replace with the token your API expects
headers = {"Authorization": f"Bearer {TOKEN}"}

S3_BUCKET = "json-assignment"
S3_PREFIX = "logistics_json/"
current_datetime = datetime.now()
dt_timestamp = current_datetime.strftime("%Y%m%d_%H%M%S")
S3_FILENAME = f"shipment_{dt_timestamp}.json"
S3_OBJECT_KEY = f"{S3_PREFIX}{S3_FILENAME}"
SNOWFLAKE_STAGE_PATH = f"@LOGISTICS_DEMO_1.BRONZE.AWS_DEMO_STAGE_3/{S3_OBJECT_KEY}"
# S3 client
s3 = boto3.client('s3')
def generate_and_upload_transactions(**context):
    from datetime import datetime
    import json, os

    # Get DAG run timestamp from context (logical date)

    
    #dt_timestamp = context['ds_nodash'] + "_" + context['ts'].split("T")[1].replace(":", "").split("+")[0]
    dt_timestamp = context['ds_nodash'] + "_" + context['ts'].split("T")[1].split(".")[0].replace(":", "")

    S3_BUCKET = "json-assignment"
    S3_PREFIX = "logistics_json/"
    S3_FILENAME = f"shipment_{dt_timestamp}.json"
    S3_OBJECT_KEY = f"{S3_PREFIX}{S3_FILENAME}"
    SNOWFLAKE_STAGE_PATH = f"@LOGISTICS_DEMO_1.BRONZE.AWS_DEMO_STAGE_3/{S3_OBJECT_KEY}"

    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()

        local_path = f"/tmp/{S3_FILENAME}"
        with open(local_path, "w") as f:
            json.dump(data, f)

        s3.upload_file(local_path, S3_BUCKET, S3_OBJECT_KEY)
        print(f"Uploaded {S3_FILENAME} to s3://{S3_BUCKET}/{S3_OBJECT_KEY}")
        print(SNOWFLAKE_STAGE_PATH)
        os.remove(local_path)

    except Exception as e:
        raise RuntimeError(f"Upload failed: {str(e)}")


default_args = {
    'start_date': datetime(2025, 6, 24),
    'retries': 1
}

with DAG(
    dag_id="2_logistics_shipment_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'snowflake', 'logistics'],
    description="Ingest JSON to S3, load to Snowflake, transform and aggregate"
) as dag:

    upload_task = PythonOperator(
        task_id="rest_api_to_s3",
        python_callable=generate_and_upload_transactions,
        provide_context=True
    )

    copy_into_task = SnowflakeOperator(
        task_id="copy_s3_to_snowflake",
        snowflake_conn_id='snowflakeid',
        sql="""
            COPY INTO LOGISTICS_DEMO_1.BRONZE.shipment_raw
            FROM '@LOGISTICS_DEMO_1.BRONZE.AWS_DEMO_STAGE_3/logistics_json/shipment_{{ ds_nodash }}_{{ ts_nodash[9:] }}.json'
            FILE_FORMAT = (TYPE = 'JSON');
        """,
    )

    insert_fact_task = SnowflakeOperator(
        task_id="json_flatten_insert_snowflake_fact",
        snowflake_conn_id='snowflakeid',
        sql="""


                MERGE INTO LOGISTICS_DEMO_1.SILVER.fact_SHIPMENTS tgt
                USING (
                    -- Deduplicate source by (order_id, carrier_id, seller_id)
                    WITH flattened_shipments AS (
                        SELECT
                            shipment.value:shipment_id::STRING AS shipment_id,
                            shipment.value AS shipment_json
                        FROM LOGISTICS_DEMO_1.BRONZE.shipment_raw,
                            LATERAL FLATTEN(input => value) shipment
                    ),
                    status_created AS (
                        SELECT
                            f.shipment_id,
                            TRY_TO_TIMESTAMP_NTZ(status.value:timestamp::STRING) AS status_created_at
                        FROM flattened_shipments f,
                            LATERAL FLATTEN(input => f.shipment_json:shipment_details.status_tracking) status
                        WHERE status.value:status::STRING = 'Created'
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.shipment_id ORDER BY status_created_at DESC) = 1
                    ),
                    status_delivered AS (
                        SELECT
                            f.shipment_id,
                            TRY_TO_TIMESTAMP_NTZ(status.value:timestamp::STRING) AS status_delivered_at
                        FROM flattened_shipments f,
                            LATERAL FLATTEN(input => f.shipment_json:shipment_details.status_tracking) status
                        WHERE status.value:status::STRING = 'Delivered'
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.shipment_id ORDER BY status_delivered_at DESC) = 1
                    ),
                    enriched_src AS (
                        SELECT
                            f.shipment_json:shipment_id::STRING AS shipment_id,
                            f.shipment_json:carrier.carrier_id::STRING AS carrier_id,
                            f.shipment_json:carrier.carrier_name::STRING AS carrier_name,
                            f.shipment_json:route.origin.city::STRING AS origin_city,
                            f.shipment_json:route.origin.pincode::STRING AS origin_pincode,
                            f.shipment_json:route.origin.warehouse_id::STRING AS warehouse_id,
                            f.shipment_json:route.destination.city::STRING AS destination_city,
                            f.shipment_json:route.destination.pincode::STRING AS destination_pincode,
                            f.shipment_json:route.destination.customer_address_type::STRING AS customer_address_type,
                            f.shipment_json:order_reference.order_id::STRING AS order_id,
                            f.shipment_json:order_reference.seller_id::STRING AS seller_id,
                            f.shipment_json:order_reference.channel::STRING AS channel,
                            f.shipment_json:charges.shipping_cost::FLOAT AS shipping_cost,
                            f.shipment_json:charges.fuel_surcharge::FLOAT AS fuel_surcharge,
                            f.shipment_json:charges.insurance::FLOAT AS insurance,
                            f.shipment_json:charges.cod_fee::FLOAT AS cod_fee,
                            f.shipment_json:shipment_details.status::STRING AS status,
                            f.shipment_json:shipment_details.rto_flag::BOOLEAN AS rto_flag,
                            f.shipment_json:shipment_details.delay_flag::BOOLEAN AS delay_flag,
                            f.shipment_json:shipment_details.delivery_tat_days::INTEGER AS delivery_tat_days,
                            TRY_TO_TIMESTAMP_NTZ(f.shipment_json:shipment_details.created_at::STRING) AS created_at,
                            TRY_TO_TIMESTAMP_NTZ(f.shipment_json:shipment_details.delivered_at::STRING) AS delivered_at,
                            sc.status_created_at,
                            sd.status_delivered_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY 
                                    f.shipment_json:order_reference.order_id::STRING,
                                    f.shipment_json:carrier.carrier_id::STRING,
                                    f.shipment_json:order_reference.seller_id::STRING
                                ORDER BY TRY_TO_TIMESTAMP_NTZ(f.shipment_json:shipment_details.created_at::STRING) DESC
                            ) AS rn
                        FROM flattened_shipments f
                        LEFT JOIN status_created sc ON f.shipment_id = sc.shipment_id
                        LEFT JOIN status_delivered sd ON f.shipment_id = sd.shipment_id
                    )
                    SELECT * FROM enriched_src WHERE rn = 1  -- ✅ Keep only latest per key
                ) src

                ON tgt.order_id = src.order_id
                AND tgt.carrier_id = src.carrier_id
                AND tgt.seller_id = src.seller_id

                WHEN MATCHED THEN UPDATE SET
                    tgt.shipment_id = src.shipment_id,
                    tgt.carrier_name = src.carrier_name,
                    tgt.origin_city = src.origin_city,
                    tgt.origin_pincode = src.origin_pincode,
                    tgt.warehouse_id = src.warehouse_id,
                    tgt.destination_city = src.destination_city,
                    tgt.destination_pincode = src.destination_pincode,
                    tgt.customer_address_type = src.customer_address_type,
                    tgt.channel = src.channel,
                    tgt.shipping_cost = src.shipping_cost,
                    tgt.fuel_surcharge = src.fuel_surcharge,
                    tgt.insurance = src.insurance,
                    tgt.cod_fee = src.cod_fee,
                    tgt.status = src.status,
                    tgt.rto_flag = src.rto_flag,
                    tgt.delay_flag = src.delay_flag,
                    tgt.delivery_tat_days = src.delivery_tat_days,
                    tgt.created_at = src.created_at,
                    tgt.delivered_at = src.delivered_at,
                    tgt.status_created_at = src.status_created_at,
                    tgt.status_delivered_at = src.status_delivered_at

                WHEN NOT MATCHED THEN INSERT (
                    shipment_id, carrier_id, carrier_name, origin_city, origin_pincode, warehouse_id,
                    destination_city, destination_pincode, customer_address_type,
                    order_id, seller_id, channel,
                    shipping_cost, fuel_surcharge, insurance, cod_fee,
                    status, rto_flag, delay_flag, delivery_tat_days,
                    created_at, delivered_at, status_created_at, status_delivered_at
                )
                VALUES (
                    src.shipment_id, src.carrier_id, src.carrier_name, src.origin_city, src.origin_pincode, src.warehouse_id,
                    src.destination_city, src.destination_pincode, src.customer_address_type,
                    src.order_id, src.seller_id, src.channel,
                    src.shipping_cost, src.fuel_surcharge, src.insurance, src.cod_fee,
                    src.status, src.rto_flag, src.delay_flag, src.delivery_tat_days,
                    src.created_at, src.delivered_at, src.status_created_at, src.status_delivered_at
                );


        """
    )



    # DAG Task Flow
    upload_task >> copy_into_task >> insert_fact_task 

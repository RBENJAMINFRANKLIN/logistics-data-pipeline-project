{{ config(
    materialized='incremental',
    unique_key='ORDER_ID',
    incremental_strategy='merge',
    transient=false
) }}

WITH raw_data AS (
    SELECT
        ORDER_ID,
        ORDER_DATE,
        PRODUCT_ID,
        CUSTOMER_ID,
        TOTAL_AMOUNT,
        PAYMENT_METHOD,
        _AIRBYTE_EXTRACTED_AT
    FROM {{ source('LOGISTICS_RAW', 'orders') }}
    {% if is_incremental() %}
    WHERE _AIRBYTE_EXTRACTED_AT > COALESCE(
    (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}),'2000-01-01')
    {% endif %}
),

invalid_orders AS (
    SELECT *,
           current_timestamp() AS error_logged_at,
           CASE
               WHEN ORDER_ID IS NULL THEN 'Missing ORDER_ID'
               WHEN ORDER_DATE IS NULL THEN 'Missing ORDER_DATE'
               WHEN CUSTOMER_ID IS NULL THEN 'Missing CUSTOMER_ID'
               WHEN PRODUCT_ID IS NULL THEN 'Missing PRODUCT_ID'
               ELSE 'Unknown error'
           END AS error_reason
    FROM raw_data
    WHERE ORDER_ID IS NULL
       OR ORDER_DATE IS NULL
       OR CUSTOMER_ID IS NULL
       OR PRODUCT_ID IS NULL
)

SELECT * FROM invalid_orders

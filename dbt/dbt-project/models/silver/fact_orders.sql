{{ config(
    materialized='incremental',
    unique_key='ORDER_ID',
    incremental_strategy='merge'
) }}

WITH raw_orders AS (
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
)

-- Final: only valid records with all required fields
SELECT *
FROM raw_orders
WHERE
    ORDER_ID IS NOT NULL
    AND ORDER_DATE IS NOT NULL
    AND CUSTOMER_ID IS NOT NULL
    AND PRODUCT_ID IS NOT NULL

{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge'
) }}

WITH raw_data AS (
    SELECT
        CUSTOMER_ID,
        NAME,
        EMAIL,
        CITY,
        STATE,
        GENDER,
        REGISTERED_AT,
        CUSTOMER_SEGMENT,
        PHONE_NUMBER,
        ADDRESS,
        _AIRBYTE_EXTRACTED_AT
    FROM {{ source('LOGISTICS_RAW', 'customers') }}
    {% if is_incremental() %}
    WHERE _AIRBYTE_EXTRACTED_AT > COALESCE(
    (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}),'2000-01-01')
    {% endif %}
),
invalid_customers AS (
    SELECT *,
           current_timestamp() AS error_logged_at,
           CASE 
               WHEN CUSTOMER_ID IS NULL THEN 'Missing CUSTOMER_ID'
               WHEN EMAIL IS NULL THEN 'Missing EMAIL'
               ELSE 'Unknown error'
           END AS error_reason
    FROM raw_data
    WHERE CUSTOMER_ID IS NULL OR EMAIL IS NULL
)

SELECT * FROM invalid_customers

{{ config(
    materialized='incremental',
    unique_key='ID',
    incremental_strategy='merge'
) }}

WITH raw_data AS (
    SELECT
        ID,
        STOCK,
        SELLER_ID,
        PRODUCT_ID,
        LAST_UPDATED,
        _AIRBYTE_EXTRACTED_AT
    FROM {{ source('LOGISTICS_RAW', 'inventory') }}
    {% if is_incremental() %}
    WHERE _AIRBYTE_EXTRACTED_AT > COALESCE(
    (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}),'2000-01-01')
    {% endif %}
),

invalid_inventory AS (
    SELECT *,
           current_timestamp() AS error_logged_at,
           CASE
               WHEN ID IS NULL THEN 'Missing ID'
               WHEN STOCK IS NULL THEN 'Missing STOCK'
               WHEN STOCK <= 20 THEN 'STOCK too low (<= 20)'
               WHEN SELLER_ID IS NULL THEN 'Missing SELLER_ID'
               WHEN PRODUCT_ID IS NULL THEN 'Missing PRODUCT_ID'
               ELSE 'Unknown error'
           END AS error_reason
    FROM raw_data
    WHERE ID IS NULL
       OR STOCK IS NULL
       OR STOCK <= 20
       OR SELLER_ID IS NULL
       OR PRODUCT_ID IS NULL
)

SELECT * FROM invalid_inventory

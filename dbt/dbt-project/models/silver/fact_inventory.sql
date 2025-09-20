{{ config(
    materialized='incremental',
    unique_key='ID',
    incremental_strategy='merge',
) }}


WITH raw_inventory AS (
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
)

-- Final output: only valid inventory records
SELECT *
FROM raw_inventory
WHERE
    ID IS NOT NULL
    AND PRODUCT_ID IS NOT NULL
    AND SELLER_ID IS NOT NULL
    AND STOCK IS NOT NULL
    AND STOCK > 20
    AND STOCK <= 5000

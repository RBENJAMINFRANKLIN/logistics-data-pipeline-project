{{ config(
    materialized='incremental',
    unique_key='PRODUCT_ID',
    incremental_strategy='merge',
    pre_hook="{{ log_audit_start(this.name, source('LOGISTICS_RAW', 'products'), '_AIRBYTE_EXTRACTED_AT') }}"
) }}

SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    PRICE,
    _AIRBYTE_EXTRACTED_AT
FROM {{ source('LOGISTICS_RAW', 'products') }}
{% if is_incremental() %}
    WHERE _AIRBYTE_EXTRACTED_AT > COALESCE(
    (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}),'2000-01-01')
{% endif %}

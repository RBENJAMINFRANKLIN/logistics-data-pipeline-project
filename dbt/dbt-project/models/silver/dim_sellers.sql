{{ config(
    materialized='incremental',
    unique_key='SELLER_ID',
    incremental_strategy='merge',
        pre_hook="{{ log_audit_start(this.name, source('LOGISTICS_RAW', 'sellers'), '_AIRBYTE_EXTRACTED_AT') }}"
) }}

SELECT
    SELLER_ID,
    SELLER_NAME,
    GST_NUMBER,
    WAREHOUSE_LOCATION,
    _AIRBYTE_EXTRACTED_AT
FROM {{ source('LOGISTICS_RAW', 'sellers') }}
{% if is_incremental() %}
    WHERE _AIRBYTE_EXTRACTED_AT > COALESCE(
    (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}),'2000-01-01')
{% endif %}

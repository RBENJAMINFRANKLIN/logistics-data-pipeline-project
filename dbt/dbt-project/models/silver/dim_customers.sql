{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge',
    pre_hook="{{ log_audit_start(this.name, source('LOGISTICS_RAW', 'customers'), '_AIRBYTE_EXTRACTED_AT') }}"
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
      (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}), '2000-01-01')
    {% endif %}
),

valid_customers AS (
    SELECT *
    FROM raw_data
    WHERE CUSTOMER_ID IS NOT NULL
      AND EMAIL IS NOT NULL
)

SELECT * FROM valid_customers

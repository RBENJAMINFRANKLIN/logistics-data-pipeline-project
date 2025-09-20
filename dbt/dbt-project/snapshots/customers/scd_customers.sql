{% snapshot scd_customers %}
{{
    config(
        target_schema='SNAPSHOT',
        unique_key='CUSTOMER_ID',
        strategy='timestamp',
        updated_at='_AIRBYTE_EXTRACTED_AT'
    )
}}

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
    -- ✅ Force cast from TZ to NTZ for dbt snapshot compatibility
    _AIRBYTE_EXTRACTED_AT::TIMESTAMP_NTZ AS _AIRBYTE_EXTRACTED_AT
FROM {{ source('LOGISTICS_RAW', 'customers') }}
WHERE CUSTOMER_ID IS NOT NULL AND EMAIL IS NOT NULL

{% endsnapshot %}

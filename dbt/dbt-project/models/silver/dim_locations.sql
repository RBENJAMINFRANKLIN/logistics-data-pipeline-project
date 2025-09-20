{{ config(
    materialized='incremental',
    unique_key='LOCATION_ID',
    incremental_strategy='merge'
) }}

SELECT
    LOCATION_ID,
    CITY,
    ZONE,
    STATE,
    PINCODE,
    _AIRBYTE_EXTRACTED_AT
FROM AIRBYTE_DATABASE.LOGISTICS.LOCATIONS

{% if is_incremental() %}
    WHERE _AIRBYTE_EXTRACTED_AT > COALESCE(
    (SELECT MAX(_AIRBYTE_EXTRACTED_AT) FROM {{ this }}),'2000-01-01')
{% endif %}

{{ config(
    materialized='incremental',
    unique_key='COURIER_ID',
    incremental_strategy='merge'
) }}

SELECT
    COURIER_ID,
    NAME,
    SUPPORT_CONTACT,
    _AB_CDC_UPDATED_AT
FROM {{ source('LOGISTICS_RAW', 'couriers') }}

{% if is_incremental() %}
    WHERE _AB_CDC_UPDATED_AT > COALESCE(
    (SELECT MAX(_AB_CDC_UPDATED_AT) FROM {{ this }}),'2000-01-01')
{% endif %}

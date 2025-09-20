
{{ config(
    materialized = 'view'
) }}

SELECT 
    s.CARRIER_NAME,
    COUNT(*) AS total_shipments,
    AVG(s.DELIVERY_TAT_DAYS) AS avg_delivery_days,
    {{ calculate_delay_percentage('s.DELAY_FLAG') }} AS delay_percentage
FROM {{ source('silver','FACT_SHIPMENTS') }} s
JOIN {{ ref('dim_couriers') }} c ON s.CARRIER_ID = c.COURIER_ID
GROUP BY s.CARRIER_NAME

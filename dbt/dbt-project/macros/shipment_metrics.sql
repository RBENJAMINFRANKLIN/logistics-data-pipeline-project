-- macros/shipment_metrics.sql
{% macro shipment_metrics(group_field) %}
SELECT 
    {{ group_field }} AS grouping_value,
    COUNT(*) AS shipment_count,
    ROUND(AVG(DELIVERY_TAT_DAYS), 2) AS avg_delivery_days,
    {{ calculate_delay_percentage('DELAY_FLAG') }} AS delay_rate
FROM {{ ref('FACT_SHIPMENTS') }}
GROUP BY {{ group_field }}
{% endmacro %}

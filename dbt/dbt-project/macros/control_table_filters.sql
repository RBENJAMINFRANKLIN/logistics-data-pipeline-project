{% macro apply_control_table_filters(table_name) %}
    SELECT
        MAX(CASE WHEN key = 'last_order_loaded_at' THEN value END) AS last_loaded_at,
        MAX(CASE WHEN key = 'enable_delay_metrics' THEN value END) AS enable_delay_metrics,
        MAX(CASE WHEN key = 'excluded_customer_ids' THEN value END) AS excluded_ids,
        MAX(CASE WHEN key = 'min_order_amount' THEN value END) AS min_order_amount,
        MAX(CASE WHEN key = 'only_registered_customers' THEN value END) AS only_registered_customers,
        MAX(CASE WHEN key = 'region_filter' THEN value END) AS region_filter
    FROM {{ ref('control_table') }}
    WHERE name = '{{ table_name }}'
{% endmacro %}

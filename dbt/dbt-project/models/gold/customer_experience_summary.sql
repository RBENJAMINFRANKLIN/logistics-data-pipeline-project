{{ 
  config(
    materialized='table',
    schema='gold'
  ) 
}}


WITH control_values AS (
    {{ apply_control_table_filters('customer_experience_summary') }}
),

excluded_customers AS (
    SELECT TRIM(value) AS customer_id
    FROM control_values,
         TABLE(SPLIT_TO_TABLE(control_values.excluded_ids, ',')) AS value
),

region_filter_values AS (
    SELECT TRIM(value) AS region
    FROM control_values,
         TABLE(SPLIT_TO_TABLE(control_values.region_filter, ',')) AS value
),

filtered_orders AS (
    SELECT o.*
    FROM {{ ref('fact_orders') }} o
    LEFT JOIN excluded_customers ec ON o.customer_id = ec.customer_id
    JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
    WHERE ec.customer_id IS NULL
      AND o.order_date > TO_TIMESTAMP((SELECT last_loaded_at FROM control_values))
      AND o.TOTAL_AMOUNT >= TRY_CAST((SELECT min_order_amount FROM control_values) AS NUMBER)
      
),

customer_summary AS (
    SELECT 
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS orders_count,
        SUM(CASE WHEN s.delay_flag THEN 1 ELSE 0 END) AS delayed_shipments,
        ROUND(AVG(DATEDIFF(DAY, o.order_date, s.delivered_at)), 2) AS avg_delivery_time
    FROM filtered_orders o
    JOIN {{ source('silver','FACT_SHIPMENTS') }} s ON o.order_id = s.order_id
    GROUP BY o.customer_id
)

SELECT *
FROM customer_summary

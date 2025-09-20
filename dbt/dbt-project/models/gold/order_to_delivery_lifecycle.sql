-- models/marts/logistics/order_to_delivery_lifecycle.sql

SELECT 
    o.ORDER_ID,
    o.ORDER_DATE,
    s.CREATED_AT AS shipment_created_at,
    s.DELIVERED_AT,
    DATEDIFF(DAY, o.ORDER_DATE, s.CREATED_AT) AS order_to_shipment_days,
    DATEDIFF(DAY, s.CREATED_AT, s.DELIVERED_AT) AS shipment_to_delivery_days,
    DATEDIFF(DAY, o.ORDER_DATE, s.DELIVERED_AT) AS total_fulfillment_days
FROM {{ ref('fact_orders') }} o
JOIN {{  source('silver','FACT_SHIPMENTS')  }} s ON o.ORDER_ID = s.ORDER_ID

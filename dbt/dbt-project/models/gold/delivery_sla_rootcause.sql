{{ config(
    materialized = 'view',
    tags = ['sla', 'logistics', 'delay_analysis']
) }}

WITH shipments_with_order AS (
    SELECT
        o.ORDER_ID,
        o.ORDER_DATE,
        s.SHIPMENT_ID,
        s.SELLER_ID,
        s.CARRIER_ID,
        s.CHANNEL,
        s.CREATED_AT AS SHIPMENT_CREATED_AT,
        s.DELIVERED_AT,
        s.DELAY_FLAG,
        s.RTO_FLAG,
        s.DELIVERY_TAT_DAYS,
        s.DESTINATION_PINCODE,
        DATEDIFF(DAY, o.ORDER_DATE, s.CREATED_AT) AS fulfillment_delay_days,
        DATEDIFF(DAY, s.CREATED_AT, s.DELIVERED_AT) AS courier_delay_days,
        DATEDIFF(DAY, o.ORDER_DATE, s.DELIVERED_AT) AS total_lag_days
    FROM {{ ref('fact_orders') }} o
    JOIN {{ source('silver', 'FACT_SHIPMENTS') }} s ON o.ORDER_ID = s.ORDER_ID
    WHERE s.STATUS = 'Delivered'
),

joined_with_dims AS (
    SELECT
        sw.ORDER_ID,
        sw.SHIPMENT_ID,
        sw.SELLER_ID,
        seller.SELLER_NAME,
        sw.CARRIER_ID,
        carrier.NAME as CARRIER_NAME,
        sw.CHANNEL,
        loc.CITY AS destination_city,
        sw.fulfillment_delay_days,
        sw.courier_delay_days,
        sw.total_lag_days,
        sw.DELAY_FLAG,
        CASE 
            WHEN sw.fulfillment_delay_days > 1 THEN 'Seller'
            WHEN sw.courier_delay_days > 2 THEN 'Courier'
            ELSE 'Unknown'
        END AS root_cause
    FROM shipments_with_order sw
    LEFT JOIN {{ ref('dim_sellers') }} seller ON sw.SELLER_ID = seller.SELLER_ID
    LEFT JOIN {{ ref('dim_couriers') }} carrier ON sw.CARRIER_ID = carrier.COURIER_ID
    LEFT JOIN {{ ref('dim_locations') }} loc ON sw.DESTINATION_PINCODE = loc.PINCODE
)

SELECT
    SELLER_NAME,
    CARRIER_NAME,
    destination_city,
    COUNT(*) AS total_deliveries,
    SUM(CASE WHEN DELAY_FLAG THEN 1 ELSE 0 END) AS delayed_shipments,
    ROUND(AVG(total_lag_days), 2) AS avg_delivery_time,
    ROUND(AVG(fulfillment_delay_days), 2) AS avg_fulfillment_lag,
    ROUND(AVG(courier_delay_days), 2) AS avg_courier_lag,
    COUNT(CASE WHEN root_cause = 'Seller' THEN 1 END) AS seller_root_causes,
    COUNT(CASE WHEN root_cause = 'Courier' THEN 1 END) AS courier_root_causes,
    CASE 
        WHEN COUNT(*) > 0 THEN ROUND(SUM(CASE WHEN DELAY_FLAG THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        ELSE 0
    END AS delay_rate_percentage
FROM joined_with_dims
GROUP BY SELLER_NAME, CARRIER_NAME, destination_city
ORDER BY delay_rate_percentage DESC

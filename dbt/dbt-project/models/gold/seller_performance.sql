
SELECT 
    s.SELLER_ID,
    sel.SELLER_NAME,
    COUNT(DISTINCT o.ORDER_ID) AS total_orders,
    COUNT(DISTINCT shp.SHIPMENT_ID) AS total_shipments,
    AVG(shp.DELIVERY_TAT_DAYS) AS avg_delivery_time,
    SUM(CASE WHEN shp.RTO_FLAG THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS rto_percentage
FROM {{ ref('fact_orders') }} o
JOIN {{  source('silver','FACT_SHIPMENTS') }} shp ON o.ORDER_ID = shp.ORDER_ID
JOIN {{ ref('fact_inventory') }} s ON o.PRODUCT_ID = s.PRODUCT_ID
JOIN {{ ref('dim_sellers') }} sel ON s.SELLER_ID = sel.SELLER_ID
GROUP BY s.SELLER_ID, sel.SELLER_NAME

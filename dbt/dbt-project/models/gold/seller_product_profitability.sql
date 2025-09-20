{{ config(
    materialized = 'view',
    tags = ['profitability', 'logistics', 'strategy']
) }}

WITH order_data AS (
    SELECT 
        o.ORDER_ID,
        o.PRODUCT_ID,
        o.CUSTOMER_ID,
        o.ORDER_DATE,
        o.TOTAL_AMOUNT,
        o.PAYMENT_METHOD,
        s.SHIPMENT_ID,
        s.SELLER_ID,
        s.CHANNEL,
        s.SHIPPING_COST,
        s.FUEL_SURCHARGE,
        s.COD_FEE,
        s.INSURANCE,
        inv.STOCK,
        inv.ID AS INVENTORY_ID,
        inv.LAST_UPDATED AS INVENTORY_LAST_UPDATED,
        c.CITY as CITY_NAME,
        c.STATE as STATE_NAME,
        c.ZONE as REGION_NAME
    FROM {{ ref('fact_orders') }} o
    JOIN {{ source('silver', 'FACT_SHIPMENTS') }} s ON o.ORDER_ID = s.ORDER_ID
    LEFT JOIN {{ ref('fact_inventory') }} inv ON o.PRODUCT_ID = inv.PRODUCT_ID AND s.SELLER_ID = inv.SELLER_ID
    LEFT JOIN {{ ref('dim_locations') }} c ON s.DESTINATION_PINCODE = c.PINCODE
),

product_seller_details AS (
    SELECT 
        od.*,
        p.PRODUCT_NAME,
        p.CATEGORY,
        sel.SELLER_NAME,
        COALESCE(od.SHIPPING_COST, 0) + COALESCE(od.FUEL_SURCHARGE, 0) +
        COALESCE(od.COD_FEE, 0) + COALESCE(od.INSURANCE, 0) AS logistics_cost,
        od.TOTAL_AMOUNT - (
            COALESCE(od.SHIPPING_COST, 0) + COALESCE(od.FUEL_SURCHARGE, 0) +
            COALESCE(od.COD_FEE, 0) + COALESCE(od.INSURANCE, 0)
        ) AS gross_profit
    FROM order_data od
    LEFT JOIN {{ ref('dim_products') }} p ON od.PRODUCT_ID = p.PRODUCT_ID
    LEFT JOIN {{ ref('dim_sellers') }} sel ON od.SELLER_ID = sel.SELLER_ID
)

SELECT
    SELLER_NAME,
    CATEGORY,
    PRODUCT_NAME,
    REGION_NAME AS DELIVERY_REGION,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    ROUND(SUM(TOTAL_AMOUNT), 2) AS revenue,
    ROUND(SUM(logistics_cost), 2) AS logistics_expense,
    ROUND(SUM(gross_profit), 2) AS gross_profit,
    ROUND(SUM(gross_profit) / NULLIF(SUM(TOTAL_AMOUNT), 0), 4) * 100 AS gross_margin_pct
FROM product_seller_details
GROUP BY
    SELLER_NAME,
    CATEGORY,
    PRODUCT_NAME,
    REGION_NAME
ORDER BY gross_margin_pct ASC

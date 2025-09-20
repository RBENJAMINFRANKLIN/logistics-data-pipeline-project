SELECT
    o.ORDER_ID,
    o.ORDER_DATE,
    c.NAME,
    c.CITY AS CUSTOMER_CITY,
    p.PRODUCT_NAME,
    s.SELLER_NAME,
    o.TOTAL_AMOUNT,
    o.PAYMENT_METHOD
FROM {{ ref('fact_orders') }} o
JOIN {{ ref('dim_customers') }} c ON o.CUSTOMER_ID = c.CUSTOMER_ID
JOIN {{ ref('dim_products') }} p ON o.PRODUCT_ID = p.PRODUCT_ID
JOIN {{ ref('dim_sellers') }} s ON p.PRODUCT_ID = s.SELLER_ID

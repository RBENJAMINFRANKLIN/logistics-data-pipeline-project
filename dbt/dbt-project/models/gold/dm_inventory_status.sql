SELECT
    i.PRODUCT_ID,
    p.PRODUCT_NAME,
    i.SELLER_ID,
    s.SELLER_NAME,
    i.STOCK,
    i.LAST_UPDATED
FROM {{ ref('fact_inventory') }} i
JOIN {{ ref('dim_products') }} p ON i.PRODUCT_ID = p.PRODUCT_ID
JOIN {{ ref('dim_sellers') }} s ON i.SELLER_ID = s.SELLER_ID

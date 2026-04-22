WITH product_variants AS (
    SELECT
        product_id
        , stock_disponible
        , actif
    FROM {{ ref('stg_products_variants')}}
)

SELECT 
    product_id
    , SUM(stock_disponible) as stock_total
    , COUNT(CASE WHEN actif = TRUE then 1 END) as nb_variantes
    , SUM(CASE WHEN stock_disponible > 0 THEN 1 ELSE 0 END) AS nb_variantes_stock
    , CASE WHEN SUM(stock_disponible) > 0 THEN true ELSE false END AS has_stock
FROM product_variants
GROUP BY product_id
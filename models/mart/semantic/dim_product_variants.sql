WITH variants AS (
    SELECT
        variant_id
      , product_id
      , sku
      , taille
      , couleur
      , stock_disponible
      , actif
    FROM {{ ref('stg_products_variants') }}
)

SELECT
    -- Clé primaire au grain variante / SKU
    variant_id

    -- Clé étrangère vers dim_products
  , product_id

    -- Attributs variante
  , sku
  , taille
  , couleur
  , stock_disponible
  , actif AS variant_actif

    -- Flags stock
  , COALESCE(stock_disponible, 0) > 0 AS has_stock
  , COALESCE(stock_disponible, 0) <= 0 AS is_rupture

FROM variants
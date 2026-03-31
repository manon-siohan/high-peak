SELECT product_id, ean13
FROM {{ ref('stg_products') }}
WHERE ean13 IS NOT NULL
  AND LENGTH(ean13) != 13
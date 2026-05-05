WITH products AS (
    SELECT
        product_id
      , product_name
      , category_id
      , categorie
      , sous_categorie
      , saison
      , supplier_id
      , supplier_name
      , supplier_pays
      , prix_vente_ht
      , prix_achat_ht
      , marge_unitaire_ht
      , taux_marge_pct
      , valid_from
      , is_current
    FROM {{ ref('int_product_current_price') }}
),

deduplicated AS (
    SELECT
        *
      , ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY 
                is_current DESC,
                valid_from DESC
        ) AS rn
    FROM products
)

SELECT
    -- Primary key
    product_id

    -- Product attributes
  , product_name
  , category_id
  , categorie
  , sous_categorie
  , saison

    -- Supplier attributes
  , supplier_id

    -- Current price attributes
  , prix_vente_ht
  , prix_achat_ht
  , marge_unitaire_ht
  , taux_marge_pct

    -- Business flags
  , CASE
        WHEN marge_unitaire_ht < 0 THEN TRUE
        ELSE FALSE
    END AS has_negative_margin

FROM deduplicated
WHERE rn = 1
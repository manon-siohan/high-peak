WITH products AS (
    SELECT * FROM {{ ref('int_product_current_price')}}
),

variants AS (
    SELECT
        variant_id
      ,  product_id
      ,  sku
      ,  taille
      ,  couleur
      ,  stock_disponible
      ,  actif
    FROM {{ ref('stg_products_variants') }}
)

SELECT
    -- Clé primaire au grain SKU
    v.variant_id                                    AS product_variant_id
  ,  v.product_id
  ,  v.sku

    -- FK
  , p.supplier_id

    -- Attributs produit
  ,  p.product_name
  ,  p.categorie
  ,  p.sous_categorie
  ,  p.saison

    -- Attributs variante
  ,  v.taille
  ,  v.couleur
  ,  v.stock_disponible
  ,  v.actif                                         AS variant_actif

    -- Prix (attributs)
  ,  p.prix_vente_ht
  ,  p.prix_achat_ht
  ,  p.marge_unitaire_ht
  ,  p.taux_marge_pct

    -- Catégorie enrichie
  ,  p.category_id

    -- Flags
  ,  v.stock_disponible > 0                          AS has_stock
  ,  v.stock_disponible is null                      AS is_rupture

FROM variants v
LEFT JOIN products p USING (product_id)

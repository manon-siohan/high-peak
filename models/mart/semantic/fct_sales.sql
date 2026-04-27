WITH sales AS (
    SELECT * FROM {{ ref('mrt_sales') }}
),
date_mapping AS (
    SELECT
        date_jour
        , date_id
    FROM {{ ref('dim_date') }}
)

SELECT
    -- Clé primaire
    s.order_item_id -- grain = ligne de commande

    -- Clés étrangères (FK vers dimensions)
    , d.date_id
    , s.customer_id
    , s.product_id
    , s.store_id
    , s.promotion_id

    -- Mesures
    , s.quantite
    , s.prix_unitaire_ht
    , s.remise_pct
    , s.ca_ligne_ht
    , s.ca_ligne_ttc
    , s.marge_ligne_ht
    , s.taux_marge_ligne_pct


    -- Flags utiles
    , s.has_promotion
    , s.is_returned

FROM sales s
LEFT JOIN date_mapping d
    ON s.date_commande = d.date_jour

WHERE s.quantite > 0
  AND s.ca_ligne_ht >= 0
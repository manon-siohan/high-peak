WITH current_ranked AS (
    SELECT
        product_id 
        , prix_vente_ht
        , valid_from 
        , is_current 
        , motif 
        ,row_number () over(
            partition by product_id
            order by valid_from DESC
        ) as rn
    FROM {{ ref("stg_price_history")}}
), 

current AS (
    SELECT * FROM current_ranked
    WHERE rn = 1
)

SELECT 
    c.product_id 
    , c.valid_from 
    , c.is_current 
    , c.motif
    , p.categorie 
    , p.product_name
    , p.prix_achat_ht
    , c.prix_vente_ht
    , c.prix_vente_ht - p.prix_achat_ht                                         AS marge_unitaire_ht
    , (c.prix_vente_ht - p.prix_achat_ht) / nullif(c.prix_vente_ht, 0) * 100    AS taux_marge_pct
FROM current c
LEFT JOIN {{ ref('stg_products')}} p
    ON c.product_id = p.product_id

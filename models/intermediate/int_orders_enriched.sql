WITH orders as (
    SELECT * FROM {{ ref('stg_orders')}}
), 

order_items as (
    SELECT * FROM {{ ref('stg_order_items')}}
),

products as (
    SELECT * FROM {{ ref('stg_products')}}
),

promotions as (
    SELECT * FROM {{ ref('stg_promotions')}}
),

returns as (
    SELECT * FROM {{ ref('stg_returns')}}
),

stores as (
    SELECT * FROM {{ ref('stg_stores')}}
),

items_aggregated as (
    SELECT
        oi.order_id
        , COUNT(*)                      as nb_lignes
        , SUM(oi.quantite)                 as quantite_totale
        , SUM(oi.montant_ligne_ht)         as montant_calcule_ht
        , SUM(oi.montant_ligne_ht * 0.20)  as tva
        , SUM(oi.montant_ligne_ht * 1.20)  as montant_calcule_ttc 
        , SUM((oi.prix_unitaire_ht - p.prix_achat_ht) * oi.quantite )  as marge_brute_ht
    FROM order_items oi
    LEFT JOIN products p USING (product_id)
    GROUP BY oi.order_id
), 

returns_flagged as (
    SELECT 
        DISTINCT order_id
    FROM returns    
), 

final as (
    SELECT
        o.order_id
        , o.customer_id
        , o.store_id
        , o.canal
        , o.statut                                                  as statut_livraison
        , s.store_name
        , s.store_pays
        , s.store_ville
        , s.store_region
        , o.date_commande
        , o.date_expedition
        , o.date_livraison
        , ia.nb_lignes
        , ia.quantite_totale
        , ia.montant_calcule_ht
        , ia.tva
        , ia.montant_calcule_ttc
        , ia.marge_brute_ht
        , o.promotion_id
        , promo.code_promo
        , promo.remise_pct
        , DATEDIFF('day', o.date_commande, o.date_expedition)       as delai_expedition_jours
        , DATEDIFF('day', o.date_expedition, o.date_livraison)      as delai_livraison_jours
        , o.canal = 'online'                                        as is_online
        , o.promotion_id IS NOT NULL                                as has_promotion
        , rf.order_id IS NOT NULL                                   as is_returned
    FROM orders o
    LEFT JOIN items_aggregated ia using (order_id)
    LEFT JOIN promotions promo using (promotion_id)
    LEFT JOIN returns_flagged rf using (order_id)
    LEFT JOIN stores s using (store_id)
)

SELECT * FROM final

with sales as (
    select * from {{ ref('mrt_sales') }}
),

date_mapping as (
    select
        date_jour
        , date_id
    from {{ ref('dim_date') }}
)

select
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
    , s.is_online
    , s.statut

from sales s
left join date_mapping d
    on s.date_commande = d.date_jour

where s.quantite > 0
  and s.ca_ligne_ht >= 0
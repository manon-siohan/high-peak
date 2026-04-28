WITH order_items AS (
    SELECT
         order_item_id
        , order_id
        , product_id 
        , variant_id 
        , sku 
        , quantite
        , prix_unitaire_ht
        , remise_pct
        , montant_ligne_ht
        , promotion_id
    FROM {{ ref('stg_order_items')}}
), 

product_price AS (
    SELECT 
    product_id 
    , valid_from 
    , is_current 
    , motif
    , categorie  
    , product_name
    , prix_achat_ht
    , prix_vente_ht
    , marge_unitaire_ht
    , taux_marge_pct
    , supplier_name
    , supplier_pays
    , delai_livraison_jours
    , supplier_specialite
    , category_id
    , sous_categorie
    , saison
    FROM {{ ref('int_product_current_price')}}
), 

orders_enriched AS (
    SELECT
        order_id
        , customer_id
        , store_id
        , canal
        , statut_livraison
        , store_name
        , store_pays
        , store_ville
        , store_region
        , date_commande
        , date_expedition
        , date_livraison
        , nb_lignes
        , quantite_totale
        , montant_calcule_ht
        , tva
        , montant_calcule_ttc
        , marge_brute_ht
        , promotion_id
        , code_promo
        , remise_pct
        , delai_expedition_jours
        , delai_livraison_jours
        , is_online
        , has_promotion
        , is_returned
    FROM {{ ref('int_orders_enriched')}}
)

SELECT
    oi.order_item_id
    , oi.order_id
    , oe.customer_id
    , oe.store_id
    , oe.store_name
    , oe.store_ville
    , oe.store_region
    , oe.store_pays
    , oe.canal
    , oe.statut
    , oe.date_commande
    , oe.date_expedition
    , oe.date_livraison
    , p.product_id
    , p.product_name
    , p.categorie
    , p.sous_categorie
    , p.saison
    , p.supplier_name
    , oe.promotion_id
    , oi.sku
    , oi.quantite
    , oi.prix_unitaire_ht
    , oi.remise_pct
    , oe.code_promo
    , oe.has_promotion
    , oe.is_returned
    , oe.is_online
    , oi.prix_unitaire_ht * oi.quantite AS ca_ligne_ht
    , (oi.prix_unitaire_ht * oi.quantite)* 1.20 AS ca_ligne_ttc
    , (oi.prix_unitaire_ht - p.prix_achat_ht)* oi.quantite AS marge_ligne_ht
    , ((oi.prix_unitaire_ht - p.prix_achat_ht)* oi.quantite)/nullif(oi.prix_unitaire_ht * oi.quantite,0)*100 AS taux_marge_ligne_pct
FROM order_items oi
LEFT JOIN orders_enriched oe USING (order_id)
LEFT JOIN product_price p USING (product_id)
WHERE oi.prix_unitaire_ht IS NOT NULL 
    AND oi.quantite IS NOT NULL
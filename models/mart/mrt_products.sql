WITH sales AS (
    SELECT
        product_id
        , COUNT(DISTINCT order_id) AS nb_commandes
        , SUM(quantite) AS total_quantite_vendue
        , SUM(ca_ligne_ht) AS total_ca_ht
        , SUM(marge_ligne_ht) AS total_marge_ht
        , SUM(marge_ligne_ht)/nullif(SUM(ca_ligne_ht),0)* 100 AS tx_total_marge_pct
        , SUM(ca_ligne_ht)/nullif(COUNT(DISTINCT order_id), 0) AS panier_moyen_produit
    FROM {{ ref('mrt_sales')}}
    GROUP BY product_id
), 

retours AS (
    SELECT
        o.product_id
        , COUNT(r.return_id)        AS nb_retours
        , SUM(r.quantite_retournee) AS quantite_retournee
    FROM {{ ref('stg_order_items')}} o
    LEFT JOIN {{ ref('stg_returns')}} r USING (order_item_id)
    GROUP BY o.product_id
), 

stock AS (
    SELECT
        product_id
        , stock_total
        , nb_variantes_stock
        , has_stock
    FROM {{ ref('int_product_variants_stock')}}
), 

info_product AS (
    SELECT 
        product_id
        , product_name
        , categorie
        , sous_categorie
        , saison
        , prix_vente_ht
        , prix_achat_ht
        , marge_unitaire_ht
        , supplier_name
        , supplier_pays
    FROM {{ ref('int_product_current_price')}}
), 

percentiles AS (
    SELECT 
        quantile_cont(total_ca_ht, 0.8) AS p80
        , quantile_cont(total_ca_ht, 0.2) AS p20
    FROM sales
)

SELECT
    i.product_id
    , i.product_name
    , i.categorie
    , i.sous_categorie
    , i.saison
    , i.prix_vente_ht
    , i.prix_achat_ht
    , i.marge_unitaire_ht
    , i.supplier_name
    , i.supplier_pays
    , s.nb_commandes
    , s.total_quantite_vendue
    , s.total_ca_ht
    , s.total_marge_ht
    , s.tx_total_marge_pct
    , s.panier_moyen_produit
    , r.nb_retours
    , r.quantite_retournee
    , r.quantite_retournee / nullif(s.total_quantite_vendue, 0)* 100 AS tx_retour_pct
    , st.stock_total
    , st.nb_variantes_stock
    , CASE WHEN st.has_stock = FALSE THEN true ELSE false END AS is_rupture
    , CASE WHEN s.total_ca_ht >= p.p80 THEN true else false END AS is_top_seller
    , CASE WHEN s.total_ca_ht <= p.P20 THEN true ELSE false END AS is_flop
FROM info_product i
LEFT JOIN sales s USING (product_id)
LEFT JOIN retours r USING (product_id)
LEFT JOIN stock st USING (product_id)
CROSS JOIN percentiles p 
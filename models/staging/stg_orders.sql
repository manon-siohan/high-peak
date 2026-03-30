WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders')}}
), 

cleaned AS (
    SELECT 
        order_id
        , customer_id
        , store_id
        , canal

        , try_cast(date_commande AS DATE) AS date_commande
        , try_cast(date_expedition AS DATE) AS date_expedition
        , try_cast(date_livraison AS DATE) AS date_livraison

        , CASE
            WHEN LOWER(TRIM(statut)) IN ('confirmée', 'confirmee','confirmé') THEN 'confirmée'
            WHEN LOWER(TRIM(statut)) IN ('expédiée', 'expedie', 'expédié', 'expedié', 'expediee') THEN 'expédiée'
            WHEN LOWER(TRIM(statut)) IN ('livrée', 'livre', 'livré', 'livree', 'livrée') THEN 'livrée'
            WHEN LOWER(TRIM(statut)) IN ('annulée', 'annulé', 'annulee', 'annulé') THEN 'annulée'
            WHEN LOWER(TRIM(statut)) IN ('remboursée', 'remboursé', 'rembourse', 'remboursee') THEN 'remboursée'
            ELSE 'inconnu'
        END AS statut 

        , try_cast(montant_total_ht AS DOUBLE) AS montant_total_ht
        , nullif(promotion_id, '')             AS promotion_id
        , try_cast(nb_articles AS INTEGER)     AS nb_articles
        
    FROM source
)

SELECT * FROM cleaned
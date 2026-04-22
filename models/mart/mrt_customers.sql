WITH customer_orders AS (
    SELECT
        customer_id
        , COUNT(DISTINCT order_id) AS nb_commandes_total
        , SUM(montant_calcule_ttc) AS total_depense
        , MAX(date_commande) AS derniere_commande_effectuee
        , SUM(quantite_totale) AS total_articles_achetes
        , SUM(CASE WHEN is_online THEN 1 ELSE 0 END) AS total_commande_en_ligne
        , SUM(CASE WHEN has_promotion THEN 1 ELSE 0 END) AS total_commande_en_promo
    FROM {{ ref('int_orders_enriched')}}
    GROUP BY customer_id
),

customer_loyalty AS( 
    SELECT * FROM (
        SELECT
            customer_id
            , solde_recalcule
            , niveau AS niveau_loyaute
            , nb_transactions AS total_loyaute_transactions
            , derniere_transaction AS derniere_activite_loyaute
            , row_number() over(
                partition by customer_id
                order by solde_recalcule DESC
            ) as rn
        FROM {{ ref('int_loyalty_balance')}}
    )
    WHERE rn = 1
)

SELECT 
    -- clients info
    ce.customer_id
    , ce.nom 
    , ce.prenom 
    , ce.email 
    , ce.telephone
    , ce.rue 
    , ce.ville
    , ce.pays 
    , ce.date_inscription
    , ce.date_naissance 
    , ce.newsletter

    -- métriques
    , co.nb_commandes_total
    , co.total_depense
    , co.derniere_commande_effectuee
    , co.total_articles_achetes
    , co.total_commande_en_ligne
    , co.total_commande_en_promo
    , CASE
        WHEN co.nb_commandes_total = 0 OR co.nb_commandes_total IS NULL THEN 0
        ELSE ROUND(co.total_depense / co.nb_commandes_total, 2)
      END AS panier_moyen
    , CASE 
        WHEN co.nb_commandes_total = 0 or co.nb_commandes_total IS NULL THEN 'Inactif'
        WHEN nb_commandes_total = 1 THEN 'Nouveau'
        WHEN nb_commandes_total < 5 THEN 'Régulier'
        ELSE 'Fidèle'
      END AS statut_achat

    -- fidélité client
    , cl.solde_recalcule
    , cl.niveau_loyaute
    , cl.total_loyaute_transactions
    , cl.derniere_activite_loyaute

    -- segments et RFM
    , ce.rfm_score
    , ce.segment_label
    , ce.segment AS customer_segment 
    , ce.total_achats_ht

    -- dates
    , DATEDIFF('day', ce.date_inscription, CURRENT_DATE) AS duree_inscription
    , DATEDIFF('day', co.derniere_commande_effectuee, CURRENT_DATE) AS duree_derniere_commande

FROM {{ ref('int_customers_enriched')}} ce 
LEFT JOIN customer_orders co USING (customer_id)
LEFT JOIN customer_loyalty cl USING (customer_id)

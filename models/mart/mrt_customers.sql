WITH main_address_ranked AS (
    SELECT
        customer_id
        , rue
        , ville
        , pays
        , row_number() over (
            partition by customer_id
            order by address_id
        ) AS rn
    FROM {{ ref('stg_addresses') }}
    WHERE type_address = 'principale'
),

main_address AS (
    SELECT
        customer_id
        , rue
        , ville
        , pays
    FROM main_address_ranked
    WHERE rn = 1
),

customer_segments_ranked AS (
    SELECT
        customer_id
        , try_cast(rfm_score AS INTEGER) AS customer_rfm_score
        , cast(segment_label AS VARCHAR) AS segment_label
        , try_cast(total_achats_ht AS DOUBLE) AS total_achats_ht
        , try_cast(nb_commandes AS INTEGER) AS nb_commandes_rfm
        , row_number() over (
            partition by customer_id
            order by date_calcul DESC
        ) AS rn
    FROM {{ ref('stg_customers_segments') }}
),

customer_segments AS (
    SELECT
        customer_id
        , customer_rfm_score
        , segment_label
        , total_achats_ht
        , nb_commandes_rfm
    FROM customer_segments_ranked
    WHERE rn = 1
),

customer_orders AS (
    SELECT
        customer_id
        , count(distinct order_id) AS nb_commandes_total
        , sum(montant_calcule_ttc) AS total_depense
        , max(date_commande) AS derniere_commande_effectuee
        , sum(quantite_totale) AS total_articles_achetes
        , sum(case when is_online then 1 else 0 end) AS total_commande_en_ligne
        , sum(case when has_promotion then 1 else 0 end) AS total_commande_en_promo
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY customer_id
),

customer_loyalty_ranked AS (
    SELECT
        customer_id
        , solde_recalcule
        , niveau AS niveau_loyaute
        , nb_transactions AS total_loyaute_transactions
        , derniere_transaction AS derniere_activite_loyaute
        , row_number() over (
            partition by customer_id
            order by solde_recalcule DESC
        ) AS rn
    FROM {{ ref('int_loyalty_balance') }}
),

customer_loyalty AS (
    SELECT
        customer_id
        , solde_recalcule
        , niveau_loyaute
        , total_loyaute_transactions
        , derniere_activite_loyaute
    FROM customer_loyalty_ranked
    WHERE rn = 1
)

SELECT
    c.customer_id
    , c.nom
    , c.prenom
    , c.email
    , c.telephone
    , ma.rue
    , ma.ville
    , ma.pays
    , c.date_inscription
    , c.date_naissance
    , try_cast(c.newsletter AS BOOLEAN) AS newsletter

    , co.nb_commandes_total
    , co.total_depense
    , co.derniere_commande_effectuee
    , co.total_articles_achetes
    , co.total_commande_en_ligne
    , co.total_commande_en_promo

    , case
        when co.nb_commandes_total = 0 or co.nb_commandes_total is null then 0
        else round(co.total_depense / co.nb_commandes_total, 2)
      end AS panier_moyen

    , case
        when co.nb_commandes_total = 0 or co.nb_commandes_total is null then 'Inactif'
        when co.nb_commandes_total = 1 then 'Nouveau'
        when co.nb_commandes_total < 5 then 'Régulier'
        else 'Fidèle'
      end AS statut_achat

    , cl.solde_recalcule
    , cl.niveau_loyaute
    , cl.total_loyaute_transactions
    , cl.derniere_activite_loyaute

    , cs.customer_rfm_score AS rfm_score
    , cs.segment_label
    , c.segment AS customer_segment
    , cs.total_achats_ht

    , datediff('day', c.date_inscription, current_date) AS duree_inscription
    , datediff('day', co.derniere_commande_effectuee, current_date) AS duree_derniere_commande

FROM {{ ref('stg_customers') }} c
LEFT JOIN main_address ma
    ON c.customer_id = ma.customer_id
LEFT JOIN customer_segments cs
    ON c.customer_id = cs.customer_id
LEFT JOIN customer_orders co
    ON c.customer_id = co.customer_id
LEFT JOIN customer_loyalty cl
    ON c.customer_id = cl.customer_id
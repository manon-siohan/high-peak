WITH source AS (
    SELECT 
        customer_id
        , prenom
        , nom
        , email
        , telephone
        , rue
        , ville
        , pays
        , date_naissance
        , date_inscription
        , newsletter
        , segment
        , rfm_score
        , segment_label
        , niveau
        , nb_commandes
    FROM {{ ref('int_customers_enriched') }}
)

select
    customer_id
   ,  prenom
   ,  nom
   ,  email
   ,  telephone
   ,  rue
   ,  ville
   ,  pays
   ,  date_naissance
   ,  date_inscription
   ,  newsletter
   ,  segment                                         AS segment_commercial
   ,  segment_label                                   AS segment_rfm
   ,  rfm_score
   ,  niveau                                          AS niveau_loyaute
    -- Statut achat (calculé depuis int)
   ,  CASE
        WHEN nb_commandes IS NULL
          OR nb_commandes = 0  THEN 'Inactif'
        WHEN nb_commandes = 1  THEN 'Nouveau'
        WHEN nb_commandes <= 4 THEN 'Régulier'
        ELSE 'Fidèle'
    END                                             AS statut_achat
    -- Ancienneté
   ,  datediff('day', date_inscription, current_date) AS anciennete_jours
   ,  date_inscription >= current_date - interval '90 days'
                                                    AS is_nouveau_client

FROM source
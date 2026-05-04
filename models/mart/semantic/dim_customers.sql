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
        , customer_segment
        , segment_label
        , try_cast(rfm_score AS INTEGER) AS rfm_score
        , niveau_loyaute
        , statut_achat
        , duree_inscription
    FROM {{ ref('mrt_customers') }}
)

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
    , customer_segment AS segment_commercial
    , segment_label AS segment_rfm
    , rfm_score
    , niveau_loyaute
    , statut_achat
    , duree_inscription AS anciennete_jours
    , date_inscription >= current_date - interval '90 days' AS is_nouveau_client
FROM source
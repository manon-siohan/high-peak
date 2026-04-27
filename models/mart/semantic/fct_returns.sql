WITH returns AS (
    SELECT
        return_id
        , order_id
        , order_item_id
        , date_retour
        , motif AS motif_retour
        , statut AS statut_retour
        , quantite_retournee
        , remboursement_ht
    FROM {{ ref('stg_returns') }}
),

sales AS (
    SELECT
        order_item_id
        , customer_id
        , product_id
        , store_id
        , promotion_id
        , quantite AS quantite_commandee
        , ca_ligne_ht
        , ca_ligne_ttc
        , marge_ligne_ht
    FROM {{ ref('fct_sales') }}
),

date_mapping AS (
    SELECT
        date_jour
        , date_id
    FROM {{ ref('dim_date') }}
),

return_reason AS (
    SELECT
        return_reason_id
        , motif_retour
    FROM {{ ref('dim_return_reason') }}
),

return_status AS (
    SELECT
        return_status_id
        , statut_retour
    FROM {{ ref('dim_return_status') }}
)

SELECT
    -- Primary key
    r.return_id

    -- Keys
    , r.order_id
    , r.order_item_id
    , d.date_id AS return_date_id
    , s.customer_id
    , s.product_id
    , s.store_id
    , s.promotion_id
    , rr.return_reason_id
    , rs.return_status_id

    -- Measures
    , r.quantite_retournee
    , r.remboursement_ht

FROM returns r
LEFT JOIN sales s
    USING(order_item_id)
LEFT JOIN date_mapping d
    ON r.date_retour = d.date_jour
LEFT JOIN return_reason rr
    USING(motif_retour)
LEFT JOIN return_status rs
    USING(statut_retour)

WHERE r.quantite_retournee > 0
  AND r.remboursement_ht >= 0
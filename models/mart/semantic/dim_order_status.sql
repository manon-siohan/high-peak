WITH statuses AS (
    SELECT DISTINCT
        statut
    FROM {{ ref('mrt_sales') }}
    WHERE statut IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['statut']) }} AS order_status_id
    , statut AS order_status_name

    , CASE
        WHEN statut IN ('confirmée', 'expédiée', 'livrée') THEN 'Active'
        WHEN statut IN ('annulée', 'remboursée') THEN 'Closed issue'
        ELSE 'Other'
      END AS order_status_group

    , CASE
        WHEN statut = 'livrée' THEN TRUE ELSE FALSE
      END AS is_delivered

    , CASE
        WHEN statut = 'annulée' THEN TRUE ELSE FALSE
      END AS is_cancelled

FROM statuses
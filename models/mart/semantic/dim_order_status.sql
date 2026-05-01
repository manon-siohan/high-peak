WITH statuses AS (
    SELECT DISTINCT
        statut_livraison
    FROM {{ ref('mrt_sales') }}
    WHERE statut_livraison IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['statut_livraison']) }} AS order_status_id
    , statut_livraison 
    , CASE
        WHEN statut_livraison IN ('confirmée', 'expédiée', 'livrée') THEN 'Active'
        WHEN statut_livraison IN ('annulée', 'remboursée') THEN 'Closed issue'
        ELSE 'Other'
      END AS order_status_group

    , CASE
        WHEN statut_livraison = 'livrée' THEN TRUE ELSE FALSE
      END AS is_delivered

    , CASE
        WHEN statut_livraison = 'annulée' THEN TRUE ELSE FALSE
      END AS is_cancelled

FROM statuses
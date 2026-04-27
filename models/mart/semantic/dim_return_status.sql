WITH statuses AS (
    SELECT DISTINCT
        statut AS statut_retour
    FROM {{ ref('stg_returns') }}
    WHERE statut IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['statut_retour']) }} AS return_status_id
    , statut_retour

    , CASE
        WHEN statut_retour = 'remboursé' THEN 'Clôturé'
        WHEN statut_retour = 'reçu' THEN 'En traitement'
        WHEN statut_retour = 'en cours' THEN 'En traitement'
        WHEN statut_retour = 'refusé' THEN 'Refusé'
        ELSE 'Autre'
      END AS return_status_category

    , CASE
        WHEN statut_retour = 'remboursé'
        THEN TRUE ELSE FALSE
      END AS is_refund

    , CASE
        WHEN statut_retour IN ('reçu', 'en cours')
        THEN TRUE ELSE FALSE
      END AS is_pending

    , CASE
        WHEN statut_retour = 'refusé'
        THEN TRUE ELSE FALSE
      END AS is_rejected

FROM statuses
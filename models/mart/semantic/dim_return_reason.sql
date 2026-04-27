WITH reasons AS (
    SELECT DISTINCT
        motif AS motif_retour
    FROM {{ ref('stg_returns') }}
    WHERE motif IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['motif_retour']) }} AS return_reason_id
    , motif_retour

    , CASE
        WHEN motif_retour = 'Défaut produit' THEN 'Qualité produit'
        WHEN motif_retour = 'Taille incorrecte' THEN 'Taille produit'
        WHEN motif_retour = 'Erreur commande' THEN 'Erreur opérationnelle'
        WHEN motif_retour = 'Délai trop long' THEN 'Problème livraison'
        WHEN motif_retour = 'Ne convient pas' THEN 'Préférence client'
        ELSE 'Autre'
      END AS categorie_retour

    , CASE
        WHEN motif_retour IN ('Défaut produit', 'Taille incorrecte', 'Ne convient pas')
        THEN TRUE ELSE FALSE
      END AS is_experience_produit

    , CASE
        WHEN motif_retour IN ('Délai trop long', 'Erreur commande')
        THEN TRUE ELSE FALSE
      END AS is_probleme_operationnel

    , CASE
        WHEN motif_retour IN ('Défaut produit', 'Erreur commande')
        THEN TRUE ELSE FALSE
      END AS is_responsabilite_entreprise

FROM reasons
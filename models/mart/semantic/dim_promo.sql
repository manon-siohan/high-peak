WITH source AS (
    SELECT * FROM {{ ref('stg_promotions')}}
)

SELECT
    promotion_id
  ,  code_promo
  ,  libelle
  ,  TRY_CAST(remise_pct AS DOUBLE)                  AS remise_pct
  ,  categories_cibles
  ,  TRY_CAST(date_debut AS DATE)                    AS date_debut
  ,  TRY_CAST(date_fin AS DATE)                      AS date_fin
    -- Durée de la promo
  ,  datediff('day',
        TRY_CAST(date_debut AS DATE),
        TRY_CAST(date_fin AS DATE))                 AS duree_jours
    -- Type de remise
  ,  case
        when TRY_CAST(remise_pct AS DOUBLE) >= 30 then 'Forte remise'
        when TRY_CAST(remise_pct AS DOUBLE) >= 15 then 'Remise modérée'
        else 'Petite remise'
    end                                             AS categorie_remise

FROM source
where TRY_CAST(remise_pct AS DOUBLE) > 0
  and TRY_CAST(remise_pct AS DOUBLE) <= 100

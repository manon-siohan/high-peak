WITH source AS (
    SELECT * FROM {{ source('raw','raw_promotions')}}
), 

cleaned AS (
    SELECT
        promotion_id
        , code_promo
        , libelle
        , CASE 
            WHEN try_cast(remise_pct AS INTEGER) < 0
            OR try_cast(remise_pct AS INTEGER) > 100 THEN NULL
            ELSE try_cast(remise_pct AS INTEGER)
        END AS remise_pct
        , categories_cibles
        , try_cast(date_debut AS DATE) as date_debut
        , try_cast(date_fin AS DATE) as date_fin
        -- type_remise : colonne inutile tout est en pourcentage
        -- statut : erreur je vais créer une nouvelle colonne
        , CASE 
            WHEN try_cast(date_fin AS DATE) IS NULL OR try_cast(date_fin AS DATE) > CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END AS active
    FROM source
)

SELECT * FROM cleaned
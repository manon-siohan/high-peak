WITH source AS (
    SELECT * FROM {{source('raw','raw_suppliers')}}
),

cleaned AS (
    SELECT
        supplier_id
        , supplier_name
        , pays
        , CASE 
            WHEN try_cast(delai_livraison_jours AS INTEGER) <= 0 THEN NULL
            ELSE try_cast(delai_livraison_jours AS INTEGER)
        END as delai_livraison_jours
        , langue_contact
        , specialite
        , try_cast(actif AS BOOL) as actif
        , siret_tva
    FROM source
)

SELECT * FROM cleaned
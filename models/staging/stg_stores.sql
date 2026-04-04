WITH source AS (
    SELECT * FROM {{ source ('raw','raw_stores')}}
), 

cleaned AS (
    SELECT 
        store_id
        , store_name
        , ville
        , region
        , CASE 
            WHEN LOWER(TRIM(pays)) IN ('france', 'fr', 'fra', 'fr.') THEN 'France'
            WHEN LOWER(TRIM(pays)) IN ('suisse', 'ch', 'che', 'switzerland') THEN 'Suisse'
            WHEN LOWER(TRIM(pays)) IN ('belgique', 'be', 'bel', 'belgium') THEN 'Belgique'
            WHEN LOWER(TRIM(pays)) IN ('luxembourg', 'lu', 'lux') THEN 'Luxembourg'
            WHEN LOWER(TRIM(pays)) IN ('allemagne', 'de', 'deu', 'germany') THEN 'Allemagne'
            ELSE pays
        END AS pays
        , type
        , CASE 
            WHEN try_cast(surface_m2 AS INTEGER) <= 0 THEN NULL
            ELSE try_cast(surface_m2 AS INTEGER)
        END AS surface_m2
        , try_cast(date_ouverture AS DATE) AS date_ouverture
        , try_cast(date_fermeture AS DATE) AS date_fermeture
        , try_cast(actif AS BOOL) AS actif
    FROM source
)

SELECT * FROM cleaned
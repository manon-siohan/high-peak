WITH source AS (
    SELECT * FROM {{ source('raw','raw_product_categories')}}
), 

cleaned AS (
    SELECT 
        category_id
        , categorie
        , 'sous-categorie' as sous_categorie
        , CASE
            WHEN LOWER(TRIM(saison)) IN ('hiver','winter','hivernal') THEN 'Hiver'
            WHEN LOWER(TRIM(saison)) IN ('printemps-automne','spring-fall','p-a') THEN 'Printemps-Automne'
            WHEN LOWER(TRIM(saison)) IN ('toutes saisons','all season','all seasons') THEN 'Toutes saisons'
            ELSE NULL
        END AS saison
    FROM source
)

SELECT * FROM cleaned
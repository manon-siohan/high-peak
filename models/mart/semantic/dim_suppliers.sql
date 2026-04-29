WITH source AS (
    SELECT * FROM {{ref('stg_suppliers')}}
)

SELECT 
    supplier_id
    , supplier_name
    , pays                      AS supplier_pays
    , specialite                AS supplier_specialite
    , delai_livraison_jours
    , actif                     AS supplier_actif
FROM source
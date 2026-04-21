WITH source AS (
    SELECT * FROM {{ source ('raw', 'raw_products')}}
), 

cleaned AS(

    SELECT
        product_id
        , product_name
        , category_id
        , CASE 
            WHEN LOWER(TRIM(categorie)) IN ('équipement', 'équipements') THEN 'Équipement'
            WHEN LOWER(TRIM(categorie)) IN ('accessoires', 'accessoire') THEN 'Accessoires'
            WHEN LOWER(TRIM(categorie)) IN ('vêtements techniques', 'vetements techniques', 'vêtements tech.') THEN 'Vêtements techniques'
            WHEN LOWER(TRIM(categorie)) IN ('chaussures', 'chaussure') THEN 'Chaussures'
            ELSE 'Autre'
        END AS categorie
        , supplier_id
        , try_cast(prix_achat_ht AS DOUBLE) AS prix_achat_ht 
        , CASE 
            WHEN try_cast(prix_vente_ht AS DOUBLE) < 0 THEN NULL
            ELSE try_cast(prix_vente_ht AS DOUBLE) 
        END AS prix_vente_ht
        , tva_pct
        , ean13
        , actif
    FROM source
)

SELECT * FROM cleaned
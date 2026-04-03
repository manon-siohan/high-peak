WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_product_variants')}}
),

cleaned AS (
    SELECT 
        variant_id
        , product_id
        , sku

        , CASE
            WHEN LOWER(TRIM(taille)) IN ('unique') THEN 'Unique'
            WHEN LOWER(TRIM(taille)) IN ('xs', 'x-small') THEN 'XS'
            WHEN LOWER(TRIM(taille)) IN ('s', 'small') THEN 'S'
            WHEN LOWER(TRIM(taille)) IN ('s/m') THEN 'S/M'
            WHEN LOWER(TRIM(taille)) IN ('m', 'medium') THEN 'M'
            WHEN LOWER(TRIM(taille)) IN ('l', 'large') THEN 'L'
            WHEN LOWER(TRIM(taille)) IN ('l/xl') THEN 'L/XL'
            WHEN LOWER(TRIM(taille)) IN ('xl', 'extra large', 'x-large') THEN 'XL'
            WHEN LOWER(TRIM(taille)) IN ('xxl', 'xx-large', '2xl') THEN 'XXL'
            WHEN LOWER(TRIM(taille)) = '38' THEN '38'
            WHEN LOWER(TRIM(taille)) = '39' THEN '39'
            WHEN LOWER(TRIM(taille)) = '40' THEN '40'
            WHEN LOWER(TRIM(taille)) = '41' THEN '41'
            WHEN LOWER(TRIM(taille)) = '42' THEN '42'
            WHEN LOWER(TRIM(taille)) = '43' THEN '43'
            WHEN LOWER(TRIM(taille)) = '44' THEN '44'
            WHEN LOWER(TRIM(taille)) = '45' THEN '45'
            WHEN LOWER(TRIM(taille)) = '46' THEN '46'
            ELSE 'Autre'
	    END AS taille

        , CASE
            WHEN LOWER(TRIM(couleur)) IN ('rouge', 'rougue', 'rouje') THEN 'Rouge'
            WHEN LOWER(TRIM(couleur)) IN ('vert', 'vert foret', 'vert forêt') THEN 'Vert'
            WHEN LOWER(TRIM(couleur)) IN ('white', 'blanc') THEN 'Blanc'
            WHEN LOWER(TRIM(couleur)) IN ('beige', 'bège') THEN 'Beige'
            WHEN LOWER(TRIM(couleur)) IN ('bleu', 'bleu marine', 'blue navy') THEN 'Bleu'
            WHEN LOWER(TRIM(couleur)) IN ('gris', 'gris anthracite') THEN 'Gris'
            WHEN LOWER(TRIM(couleur)) IN ('noir', 'noire') THEN 'Noir'
            WHEN LOWER(TRIM(couleur)) IN ('orange', 'orangé') THEN 'Orange'
            ELSE 'Autre'
	    END AS couleur

        , CASE 
            WHEN try_cast(stock_disponible AS INTEGER) < 0 THEN NULL 
            ELSE try_cast(stock_disponible AS INTEGER) 
	    END AS stock_disponible
        , actif
    FROM source
)

SELECT * FROM cleaned
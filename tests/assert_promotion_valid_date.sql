{{ config(severity= 'warn') }}

SELECT 
    promotion_id
FROM {{ ref('stg_promotions')}}
WHERE date_fin IS NOT NULL 
    AND date_fin < date_debut
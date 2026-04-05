{{ config( severity = 'warn') }}

SELECT
    store_id
FROM {{ ref('stg_stores')}}
WHERE date_fermeture IS NOT NULL 
    AND date_fermeture < date_ouverture
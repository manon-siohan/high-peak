{{
  config(
    materialized = 'table',
    )
}}

WITH source AS(
    SELECT * FROM {{ source('raw','raw_price_history')}}
), 

cleaned AS (
    SELECT
        price_history_id
        , product_id 
        , try_cast(prix_vente_ht AS DOUBLE) AS prix_vente_ht
        , try_cast(valid_from AS DATE) AS valid_from
        , try_cast(valid_to AS DATE) AS valid_to
        , try_cast(is_current AS BOOL) AS is_current
        , motif
    FROM source
)

SELECT * FROM cleaned
WITH source AS(
    SELECT * FROM {{ source('raw','raw_returns')}}
), 

cleaned AS (
    SELECT
        return_id
        , order_id
        , order_item_id
        , try_cast(date_retour AS DATE) AS date_retour
        , motif 
        , try_cast(quantite_retournee AS INTEGER) as quantite_retournee
        , statut 
        , try_cast(remboursement_ht AS DOUBLE) as remboursement_ht
    FROM source
)

SELECT * FROM cleaned
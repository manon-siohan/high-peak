WITH source AS (
    SELECT * FROM {{ source('raw','raw_order_items')}}
),

cleaned AS (
    SELECT
        order_item_id
        , order_id
        , product_id 
        , variant_id 
        , sku 
        , CASE 
            WHEN try_cast(quantite AS INTEGER) <=0 THEN NULL
            ELSE try_cast(quantite AS INTEGER)
        END as quantite
        , CASE 
            WHEN try_cast(prix_unitaire_ht AS DOUBLE) < 0 THEN NULL
            ELSE try_cast(prix_unitaire_ht AS DOUBLE)
        END as prix_unitaire_ht
        , CASE
            WHEN try_cast(remise_pct AS DOUBLE) > 100
            OR try_cast(remise_pct AS DOUBLE) < 0 THEN NULL
        END as remise_pct
        , CASE 
            WHEN try_cast(montant_ligne_ht AS DOUBLE) < 0 THEN NULL
            ELSE try_cast(montant_ligne_ht AS DOUBLE) 
        END as montant_ligne_ht
        , promotion_id
    FROM source
)

SELECT * FROM cleaned
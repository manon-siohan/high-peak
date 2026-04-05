WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_loyalty_transactions') }}
), 

cleaned AS (
    SELECT
        transaction_id
        , loyalty_account_id
        , customer_id
        , CASE 
            WHEN LOWER(TRIM(type)) IN ('earn','gagné','credit') THEN 'Crédité'
            WHEN LOWER(TRIM(type)) IN ('redeem', 'dépensé', 'utilisé', 'débit', 'debit') THEN 'Débité'
            WHEN LOWER(TRIM(type)) IN ('expire', 'expiré', 'expiry') THEN 'Expiré'
            WHEN LOWER(TRIM(type)) IN ('bonus') THEN 'Bonus'
            ELSE 'Autre'
        END AS type_transaction
        , nullif(try_cast(points AS INTEGER),0) as points
        , try_cast(date_transaction AS DATE) as date_transaction
        , order_id
        , description as description_transaction
    FROM source
)

SELECT * FROM cleaned
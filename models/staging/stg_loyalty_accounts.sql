WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_loyalty_accounts') }}
), 

cleaned AS (
    SELECT
        loyalty_account_id 
        , customer_id 
        , CASE 
            WHEN try_cast(points_solde AS INTEGER) < 0 THEN NULL
            ELSE try_cast(points_solde AS INTEGER)
        END as points_solde
        , CASE 
            WHEN try_cast(points_solde AS INTEGER) IS NULL THEN NULL
            WHEN try_cast(points_solde AS INTEGER) < 3000 THEN 'Bronze'
            WHEN try_cast(points_solde AS INTEGER) < 10000 THEN 'Silver'
            WHEN try_cast(points_solde AS INTEGER) < 30000 THEN 'Gold'
            WHEN try_cast(points_solde AS INTEGER) >= 30000 THEN 'Platinum'
            ELSE 'Autre'
        END AS niveau
        , try_cast(date_adhesion AS DATE) as date_adhesion
        , try_cast(actif AS BOOL) as active
    FROM source
)

SELECT * FROM cleaned
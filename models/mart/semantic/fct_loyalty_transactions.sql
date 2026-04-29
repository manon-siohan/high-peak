WITH loyalty_transactions AS (
    SELECT
        transaction_id
        , loyalty_account_id
        , customer_id
        , type_transaction
        , points
        , date_transaction
        , order_id
    FROM {{ ref('stg_loyalty_transactions') }}
),

date_mapping AS (
    SELECT
        date_jour
        , date_id
    FROM {{ ref('dim_date') }}
),

transaction_type AS (
    SELECT
        loyalty_transaction_type_id
        , type_transaction
    FROM {{ ref('dim_loyalty_transaction_type') }}
)

SELECT
    
    lt.transaction_id -- Primary key
    , lt.loyalty_account_id
    , lt.order_id
    , d.date_id AS transaction_date_id
    , lt.customer_id
    , tt.loyalty_transaction_type_id
    -- Measures
    , lt.points

FROM loyalty_transactions lt
LEFT JOIN date_mapping d
    ON lt.date_transaction = d.date_jour
LEFT JOIN transaction_type tt
    USING(type_transaction)

WHERE lt.points IS NOT NULL
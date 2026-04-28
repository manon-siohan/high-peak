WITH transactions AS (
    SELECT
        transaction_id
        , loyalty_account_id
        , customer_id
        , type_transaction
        , points
        , date_transaction
        , order_id
        , description_transaction
    FROM {{ ref('stg_loyalty_transactions')}}
)

SELECT
    loyalty_account_id
    , SUM(CASE 
        WHEN type_transaction IN ('Crédité', 'Bonus') 
        THEN points ELSE 0 
      END) AS total_points_gagnes

    , SUM(CASE 
        WHEN type_transaction = 'Débité' 
        THEN ABS(points) ELSE 0 
      END) AS total_points_depenses

    , SUM(CASE 
        WHEN type_transaction = 'Expiré' 
        THEN ABS(points) ELSE 0 
      END) AS total_points_expires

    , SUM(CASE 
            WHEN type_transaction = 'Débité' 
            THEN ABS(points) ELSE 0 
          END)
      / NULLIF(
          SUM(CASE 
                WHEN type_transaction IN ('Crédité', 'Bonus') 
                THEN points ELSE 0 
              END), 0) * 100 AS tx_utilisation_pct
FROM transactions
GROUP BY loyalty_account_id

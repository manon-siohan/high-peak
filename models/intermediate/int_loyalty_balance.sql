WITH accounts AS (
    SELECT * FROM {{ ref('stg_loyalty_accounts')}}
), 

transactions AS (
    SELECT * FROM {{ ref('stg_loyalty_transactions')}}
), 

transactions_aggregated AS (
    SELECT 
        loyalty_account_id
        , SUM(points) AS solde_recalcule
        , COUNT(*) AS nb_transactions
        , MAX(date_transaction) AS derniere_transaction
    FROM transactions
    GROUP BY loyalty_account_id
), 

final AS (
    SELECT 
        a.loyalty_account_id
        , a.customer_id
        , a.points_solde AS solde_declare
        , COALESCE(t.solde_recalcule, 0) AS solde_recalcule
        , a.points_solde - t.solde_recalcule AS ecart_solde
        , a.niveau
        , a.date_adhesion
        , COALESCE(t.nb_transactions, 0) AS nb_transactions
        , t.derniere_transaction
    FROM accounts a 
    LEFT JOIN transactions_aggregated t USING (loyalty_account_id)
)

SELECT * FROM final
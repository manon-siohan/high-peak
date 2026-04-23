SELECT
    b.loyalty_account_id
    , b.customer_id
    , b.solde_recalcule
    , b.ecart_solde
    , b.niveau
    , b.date_adhesion
    , b.nb_transactions
    , b.derniere_transaction
    , t.total_points_gagnes
    , t.total_points_depenses
    , t.total_points_expires
    , t.tx_utilisation_pct
    , DATEDIFF('day', b.derniere_transaction, CURRENT_DATE) AS duree_derniere_transaction
    , CASE 
        WHEN DATEDIFF('day', b.derniere_transaction, CURRENT_DATE) <= 180
        THEN TRUE ELSE FALSE
     END AS is_actif
FROM {{ ref('int_loyalty_balance')}} b 
LEFT JOIN {{ ref('int_loyalty_transactions')}} t USING(loyalty_account_id)
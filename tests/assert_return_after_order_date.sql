{{ config (severity = 'warn')}}

SELECT
    r.return_id
FROM {{ ref('stg_returns')}} r 
JOIN {{ ref('stg_orders')}} o 
    USING (order_id)
WHERE r.date_retour < o.date_commande
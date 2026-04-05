{{ config (severity ='warn')}}

SELECT 
    return_id
FROM {{ ref('stg_returns')}} r 
JOIN {{ ref('stg_order_items')}} oi 
    USING(order_item_id)
WHERE r.quantite_retournee > oi.quantite
WITH types AS (
    SELECT DISTINCT
        type_transaction
    FROM {{ ref('stg_loyalty_transactions') }}
    WHERE type_transaction IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['type_transaction']) }} AS loyalty_transaction_type_id
    , type_transaction

    , CASE
        WHEN type_transaction = 'Crédité' THEN 'Gain'
        WHEN type_transaction = 'Débité' THEN 'Dépense'
        WHEN type_transaction = 'Expiré' THEN 'Expiration'
        WHEN type_transaction = 'Bonus' THEN 'Promotion'
        ELSE 'Other'
      END AS transaction_category

    , CASE
        WHEN type_transaction IN ('Crédité', 'Bonus')
        THEN TRUE ELSE FALSE
      END AS is_earning

    , CASE
        WHEN type_transaction = 'Débité'
        THEN TRUE ELSE FALSE
      END AS is_spending

    , CASE
        WHEN type_transaction = 'Expiré'
        THEN TRUE ELSE FALSE
      END AS is_expiration

FROM types
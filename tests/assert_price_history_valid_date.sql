{{ config(severity='warn') }}

SELECT price_history_id
FROM {{ ref('stg_price_history') }}
WHERE valid_to IS NOT NULL
  AND valid_to < valid_from
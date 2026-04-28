WITH channels AS (
    SELECT DISTINCT
        canal
    FROM {{ ref('mrt_sales') }}
    WHERE canal IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['canal']) }} AS channel_id
    , canal 

    , CASE
        WHEN canal = 'online' THEN 'Digital'
        WHEN canal = 'marketplace' THEN 'Digital'
        WHEN canal = 'magasin' THEN 'Retail'
        ELSE 'Other'
      END AS canal_group

    , CASE
        WHEN canal IN ('online', 'marketplace')
        THEN TRUE ELSE FALSE
      END AS is_digital_canal

FROM channels
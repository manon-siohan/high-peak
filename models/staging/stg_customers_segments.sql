{{ config(materialized='table')}}

WITH raw_customer_segments AS (
    SELECT
        segment_id
        , customer_id
        , r_score AS raw_r_score
        , f_score AS raw_f_score
        , m_score AS raw_m_score
        , rfm_score AS raw_rfm_score
        , segment_label AS raw_segment_label
        , date_calcul AS raw_date_calcul
        , total_achats_ht AS raw_total_achats_ht
        , nb_commandes AS raw_nb_commandes
    FROM {{ source('raw', 'raw_customer_segments') }}
),

cleaned AS ( 
    SELECT
        segment_id
        , customer_id

        , CASE
            WHEN try_cast(raw_r_score AS INTEGER) BETWEEN 1 AND 5 
            THEN try_cast(raw_r_score AS INTEGER)
            ELSE NULL
          END AS r_score

        , CASE
            WHEN try_cast(raw_f_score AS INTEGER) BETWEEN 1 AND 5 
            THEN try_cast(raw_f_score AS INTEGER)
            ELSE NULL
          END AS f_score

        , CASE
            WHEN try_cast(raw_m_score AS INTEGER) BETWEEN 1 AND 5 
            THEN try_cast(raw_m_score AS INTEGER)
            ELSE NULL
          END AS m_score

        , try_cast(raw_rfm_score AS INTEGER) AS rfm_score
        , cast(raw_segment_label AS VARCHAR) AS segment_label
        , try_cast(raw_date_calcul AS DATE) AS date_calcul

        , CASE
            WHEN try_cast(raw_total_achats_ht AS DOUBLE) < 0 THEN NULL
            ELSE try_cast(raw_total_achats_ht AS DOUBLE)
          END AS total_achats_ht

        , try_cast(raw_nb_commandes AS INTEGER) AS nb_commandes

    FROM raw_customer_segments
)

SELECT
    segment_id
    , customer_id
    , r_score
    , f_score
    , m_score
    , rfm_score
    , segment_label
    , date_calcul
    , total_achats_ht
    , nb_commandes
FROM cleaned
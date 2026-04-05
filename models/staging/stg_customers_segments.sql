WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_customer_segments')}}
), 

cleaned AS ( 
    SELECT
        segment_id
        , customer_id
        , CASE
            WHEN try_cast(r_score AS INTEGER) BETWEEN 1 AND 5 
            THEN try_cast(r_score AS INTEGER)
            ELSE NULL
        END AS r_score
        , CASE
            WHEN try_cast(f_score AS INTEGER) BETWEEN 1 AND 5 
            THEN try_cast(f_score AS INTEGER)
            ELSE NULL
        END AS f_score
        , CASE
            WHEN try_cast(m_score AS INTEGER) BETWEEN 1 AND 5 
            THEN try_cast(m_score AS INTEGER)
            ELSE NULL
        END AS m_score
        , try_cast(rfm_score AS INTEGER) as rfm_score
        , segment_label
        , try_cast(date_calcul AS DATE) as date_calcul
        , CASE
            WHEN try_cast(total_achats_ht AS DOUBLE) < 0 THEN NULL
            ELSE try_cast(total_achats_ht AS DOUBLE)
        END as total_achats_ht
        , try_cast(nb_commandes AS INTEGER) as nb_commandes
    FROM source
)

SELECT * FROM cleaned
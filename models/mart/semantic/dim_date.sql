WITH date_cal AS (
    SELECT
        RANGE:: date_day
    FROM RANGE(
        DATE '2021-01-01'
        ,DATE '2025-12-31'
        ,interval '1 day'
    )
),

final AS (
    SELECT
        -- Clé primaire format DDMMYYYY
        CAST(strftime(date_day, '%d%m%Y') AS INTEGER)   AS date_id
        , date_day                                         AS date_jour

        -- Attributs temporels
        , YEAR(date_day)                                   AS annee
        , QUARTER(date_day)                                AS trimestre
        , 'T' || QUARTER(date_day)                         AS trimestre_label
        , MONTH(date_day)                                  AS mois
        , strftime(date_day, '%B')                         AS mois_label
        , WEEK(date_day)                                   AS semaine
        , dayofweek(date_day)                              AS jour_semaine
        , strftime(date_day, '%A')                         AS jour_semaine_label
        -- Formats utiles Power BI
        , strftime(date_day, '%Y-%m')                      AS annee_mois
        , annee || '-T' || QUARTER(date_day)               AS annee_trimestre

        -- Flags
        , dayofweek(date_day) IN (0, 6)                   AS is_weekend

        -- Mois en cours / passé (utile pour DAX)
        , date_day = DATE_TRUNC('month', current_date)     AS is_current_month
        , YEAR(date_day) = YEAR(current_date)              AS is_current_year

    FROM date_cal
)

SELECT * FROM final
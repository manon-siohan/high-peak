with date_spine as (
    select
        range::date as date_day
    from range(
        date '2021-01-01'
        ,date '2025-12-31'
        ,interval '1 day'
    )
),

final as (
    select
        -- Clé primaire format DDMMYYYY
        cast(strftime(date_day, '%d%m%Y') as integer)   as date_id
        , date_day                                         as date_jour

        -- Attributs temporels
        , year(date_day)                                   as annee
        , quarter(date_day)                                as trimestre
        , 'T' || quarter(date_day)                         as trimestre_label
        , month(date_day)                                  as mois
        , strftime(date_day, '%B')                         as mois_label
        , week(date_day)                                   as semaine
        , dayofweek(date_day)                              as jour_semaine
        , strftime(date_day, '%A')                         as jour_semaine_label
        -- Formats utiles Power BI
        , strftime(date_day, '%Y-%m')                      as annee_mois
        , annee || '-T' || quarter(date_day)               as annee_trimestre

        -- Flags
        , dayofweek(date_day) in (0, 6)                   as is_weekend

        -- Mois en cours / passé (utile pour DAX)
        , date_day = date_trunc('month', current_date)     as is_current_month
        , year(date_day) = year(current_date)              as is_current_year

    from date_spine
)

select * from final
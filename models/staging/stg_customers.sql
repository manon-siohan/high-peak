with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

cleaned as (
    select
        customer_id,
        trim(prenom)                    as prenom,
        trim(nom)                       as nom,

        -- nettoyage email
        case
            when email is null          then null
            when trim(email) = ''       then null
            when email not like '%@%'   then null
            else lower(trim(email))
        end                             as email,

        -- nettoyage téléphone
        nullif(trim(telephone), '')     as telephone,
        trim(ville)                     as ville,

        -- normalisation pays
        case
            when lower(trim(pays)) in ('france','fr','fra','fr.') then 'France'
            when lower(trim(pays)) in ('suisse','ch','che','switzerland') then 'Suisse'
            when lower(trim(pays)) in ('belgique','be','bel','belgium') then 'Belgique'
            when lower(trim(pays)) in ('luxembourg','lu','lux') then 'Luxembourg'
            when lower(trim(pays)) in ('allemagne','de','deu','germany') then 'Allemagne'
            else pays
        end                             as pays,

        -- normalisation date avec formats multiples
        case
            when regexp_matches(date_inscription, '^\d{4}-\d{2}-\d{2}$')
                then try_cast(date_inscription as date)
            when regexp_matches(date_inscription, '^\d{2}/\d{2}/\d{4}$')
                then try_cast(
                    split_part(date_inscription,'/',3) || '-' ||
                    split_part(date_inscription,'/',2) || '-' ||
                    split_part(date_inscription,'/',1) as date)
            when regexp_matches(date_inscription, '^\d{2}-\d{2}-\d{4}$')
                then try_cast(
                    split_part(date_inscription,'-',3) || '-' ||
                    split_part(date_inscription,'-',1) || '-' ||
                    split_part(date_inscription,'-',2) as date)
            when regexp_matches(date_inscription, '^\d{8}$')
                then try_cast(
                    substr(date_inscription,1,4) || '-' ||
                    substr(date_inscription,5,2) || '-' ||
                    substr(date_inscription,7,2) as date)
            else null
        end                             as date_inscription,

        -- même logique pour date_naissance
        case
            when regexp_matches(date_naissance, '^\d{4}-\d{2}-\d{2}$')
                then try_cast(date_naissance as date)
            when regexp_matches(date_naissance, '^\d{2}/\d{2}/\d{4}$')
                then try_cast(
                    split_part(date_naissance,'/',3) || '-' ||
                    split_part(date_naissance,'/',2) || '-' ||
                    split_part(date_naissance,'/',1) as date)
            when regexp_matches(date_naissance, '^\d{2}-\d{2}-\d{4}$')
                then try_cast(
                    split_part(date_naissance,'-',3) || '-' ||
                    split_part(date_naissance,'-',1) || '-' ||
                    split_part(date_naissance,'-',2) as date)
            when regexp_matches(date_naissance, '^\d{8}$')
                then try_cast(
                    substr(date_naissance,1,4) || '-' ||
                    substr(date_naissance,5,2) || '-' ||
                    substr(date_naissance,7,2) as date)
            else null
        end                             as date_naissance,

        try_cast(newsletter as integer) as newsletter,
        trim(segment)                   as segment

    from source
)

select * from cleaned
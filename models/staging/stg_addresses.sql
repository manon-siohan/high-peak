WITH source AS (
    SELECT * FROM {{ source('raw','raw_addresses')}}
), 

cleaned AS (
    SELECT
        address_id
        , customer_id
        , type as type_address
        , rue 
        , regexp_replace(lower(trim(ville)), '(^|\s|-)([a-zàâäéèêëîïôùûüç])', '\1\U\2') as ville
        , code_postal
        , case
            when lower(trim(pays)) in ('france','fr','fra','fr.') then 'France'
            when lower(trim(pays)) in ('suisse','ch','che','switzerland') then 'Suisse'
            when lower(trim(pays)) in ('belgique','be','bel','belgium') then 'Belgique'
            when lower(trim(pays)) in ('luxembourg','lu','lux') then 'Luxembourg'
            when lower(trim(pays)) in ('allemagne','de','deu','germany') then 'Allemagne'
            else pays
        end as pays
        , try_cast(actif AS BOOL) as active

    FROM source
)

SELECT * FROM cleaned
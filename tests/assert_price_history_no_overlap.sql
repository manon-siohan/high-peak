{{ config(severity='warn') }}

select a.price_history_id
from {{ ref('stg_price_history') }} a
join {{ ref('stg_price_history') }} b
    on a.product_id = b.product_id
    and a.price_history_id != b.price_history_id
where a.valid_from < coalesce(b.valid_to, '9999-12-31')
    and coalesce(a.valid_to, '9999-12-31') > b.valid_from
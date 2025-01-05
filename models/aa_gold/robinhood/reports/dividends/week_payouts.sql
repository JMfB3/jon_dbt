{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    dividends as (select * from {{ ref('dividends') }}),

    base as (
        select *
        from dividends 
        where 
            coalesce(paid_at, '1990-01-01') > date_trunc(year, current_date())
    ), 

    final as (
        select 
            ticker as ticker,
            to_char(amount, '$999,999,999,990.00') as amount,
            to_char(date_trunc(day, paid_at), 'YYYY-MM-DD') as paid_at
        from base 
        where 
            paid_at > current_date() - 7
        order by 2 desc
    )

select * 
from final 

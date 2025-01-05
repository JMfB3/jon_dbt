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

    records as (
        select 
            ticker,
            amount,
            to_char(date_trunc(week, paid_at), 'YYYY-MM-DD') as wk,
            rank() over(order by wk desc) as r
        from base 
        where 
            paid_at > current_date() - 7
        qualify r = 1
        order by wk 
    ),

    final as (
        select to_char(sum(amount), '$999,999,999,990.00') as week_divs_paid
        from records
    )

select * 
from final 

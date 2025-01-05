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

    tickers as (
        select distinct 
            ticker as ticker,
        from base 
        where 
            paid_at > current_date() - 7
    ),

    final as (
        select 
            tickers.ticker,
            to_char(sum(base.amount), '$999,999,999,990.00') as amount,
        from base 
        inner join tickers on base.ticker = tickers.ticker
        group by 1
    )

select *
from final 

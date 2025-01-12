{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    dividends as (select * from {{ ref('dividends') }}), 
    
    tickers as (
        select  
            ticker,
            to_char(sum(dividends.amount), '$999,999,999,990.00') as week_payout,
        from dividends 
        where 
            date_trunc(week, paid_at) = date_trunc(week, current_date() - 2)
            and 
            status not in ('voided', 'pending')
        group by 1
    ),

    final as (
        select 
            tickers.ticker,
            tickers.week_payout,
            to_char(sum(dividends.amount), '$999,999,999,990.00') as ytd_payout,
        from tickers 
        left join dividends on tickers.ticker = dividends.ticker 
        where 
            dividends.paid_at >= date_trunc(year, current_date())
            and 
            status not in ('voided', 'pending')
        group by 1,2
    )

select *
from final 
order by 2, 1

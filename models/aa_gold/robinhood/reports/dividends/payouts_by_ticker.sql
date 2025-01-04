{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    dividends as (select * from {{ ref('dividends') }}),

    final as (
        select 
            ticker,
            -- sum(amount) as total_paid_out,
            to_char(sum(amount), '$999,999,999,990.00') as total_paid_out, 
            to_date(max(paid_at)) as last_payout_date
        from dividends
        where 
            paid_at >= '2023-01-01'
            and 
            status not in ('voided', 'pending')
        group by 1
    )

select *
from final 
order by 2 desc

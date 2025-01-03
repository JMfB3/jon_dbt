{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    dividends as (select * from {{ ref('dividends') }}),

    final as (
        select 
            to_char(date_trunc('month', coalesce(paid_at, payable_date)), 'YYYY-MM') as mmm,
            to_char(sum(amount), '$999,999,999,990.00') as dividends
        from dividends
        where 
            paid_at >= '2023-01-01'
        group by 1
    )

select *
from final 
order by 1 desc 

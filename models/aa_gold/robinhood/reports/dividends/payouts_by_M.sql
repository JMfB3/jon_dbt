{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    dividends as (select * from {{ ref('dividends') }}),

    final as (
        select 
            to_char(date_trunc(month, paid_at), 'YYYY-MM') as mmm,
            to_char(sum(amount), '$999,999,999,990.00') as dividends
        from dividends
        where 
            coalesce(paid_at, '9999-12-31') between '2023-01-01' and current_date()
            and 
            status not in ('voided', 'pending')
        group by 1
    )

select *
from final 
order by 1 desc 

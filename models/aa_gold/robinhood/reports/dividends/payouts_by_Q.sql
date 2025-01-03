{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    dividends as (select * from {{ ref('dividends') }}),

    a as (
        select 
            right(date_part('year', coalesce(paid_at, payable_date)), 2) || 
                '-Q' || 
                date_part('quarter', coalesce(paid_at, payable_date)) qqq,
            sum(amount) as divs,
            to_char(sum(amount), '$999,999,999,990.00') as fmt_divs, 
        from dividends
        group by 1
        order by 1 desc 
    ),

    b as (
        select *,
            lag(divs) over(order by qqq) as l
        from a 
        order by qqq desc 
    ), 

    final as (
        select 
            b.qqq,
            b.fmt_divs as dividends,
            to_char(round(((b.divs - b.l) / b.l ), 2) * 100, 'FM999999990') || '%' as pct_chg
        from b 
    )

select *
from final 

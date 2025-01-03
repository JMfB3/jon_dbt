{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    base as (select * from {{ ref('dividends') }}),

    final as (
        select to_char(sum(amount), '$999,999,999,990.00') as dividends
        from base 
        where 
            paid_at >= '2023-01-01' 
    )

select *
from final 


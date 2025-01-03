{{ config(
    materialized='view',
    tags=['reports'],
    query_tag='REPORTS',
) }}

with 
    deposits as (select * from {{ ref('bnk_txfrs') }}),
    -- NEED TO EXCLUDE TXFRS OUT!!! 
    final as (
        select 
            date_trunc('year', created_at) as deposited_year,
            to_char(sum(amount), '$999,999,999,990.00') as amount,
        from robinhood.public.bnk_txfrs
        where 
            created_at >= '2023-01-01'
        group by 1 
    )

select *
from final 

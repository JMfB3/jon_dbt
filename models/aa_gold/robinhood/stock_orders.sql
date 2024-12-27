{{ config(
    materialized='table',
    query_tag='DBT_GOLD_ROBINHOOD',
)}}

with 
    final as (select * from {{ ref('stg__stock_orders') }})

select * 
from final 
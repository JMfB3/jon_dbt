{{ config(
    materialized='view',
    tags=['stg'],
    query_tag="DBT__STAGING__ROBINHOOD",
    pre_hook=[
        "create stage if not exists {{ env_var('DBT_DB') }}.jons_stages.rh__stock_orders 
            url = {{ env_var('DBT_RH_STAGE') }}stock_orders/'
            credentials = (aws_key_id = '{{ env_var('DBT_AWS_KEY') }}', aws_secret_key = '{{ env_var('DBT_AWS_SECRET') }}');
        copy into {{ source('raw_robinhood', 'stock_orders') }} 
        from (
            select 
                t.$1 as data,
                METADATA$FILE_ROW_NUMBER row_num,
                METADATA$FILENAME as filename,
                METADATA$START_SCAN_TIME as log_ingest_date,
                METADATA$FILE_LAST_MODIFIED as file_last_modified_date,
            from @{{ env_var('DBT_DB') }}.jons_stages.rh__stock_orders t)
        file_format = (type = json)
        on_error = 'SKIP_FILE' "]
    )
}}

-- Bronze ETL above in Pre-Hook
-- Silver ETL below 

with 
    base as (select * from {{ source('raw_robinhood', 'stock_orders') }}), 

    all_stg_data as (
        select distinct
            value['id']::string as robinhood_stock_order_id,
            value['tckr']::string as ticker,
            value['average_price']::number(38, 2) as average_price,
            value['state']::string as status,
            value['side']::string as side,
            value['quantity']::number(38, 2) as quantity,
            value['created_at']::datetime as created_at,
            value['updated_at']::datetime as updated_at,
            log_ingest_date,
            row_number() over(partition by robinhood_stock_order_id order by log_ingest_date desc) as r
        from base,
        lateral flatten(input => parse_json(base.data))
    ), 
    
    final as (
        select all_stg_data.* exclude(r)
        from all_stg_data 
        where all_stg_data.r = 1
    )
    
select *
from final 

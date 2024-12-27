{{ config(
    materialized='view',
    tags=['stg'],
    query_tag="DBT__STAGING__ROBINHOOD",
    pre_hook=[
        "create stage if not exists {{ env_var('DBT_DB') }}.jons_stages.rh__dividends 
            url = {{ env_var('DBT_RH_STAGE') }}dividends/'
            credentials = (aws_key_id = '{{ env_var('DBT_AWS_KEY') }}', aws_secret_key = '{{ env_var('DBT_AWS_SECRET') }}');
        copy into {{ source('raw_robinhood', 'dividends') }} 
        from (
            select 
                t.$1 as data,
                METADATA$FILE_ROW_NUMBER row_num,
                METADATA$FILENAME as filename,
                METADATA$START_SCAN_TIME as log_ingest_date,
                METADATA$FILE_LAST_MODIFIED as file_last_modified_date,
            from @{{ env_var('DBT_DB') }}.jons_stages.rh__dividends t)
        file_format = (type = json)
        on_error = 'SKIP_FILE' "]
    )
}}

-- Bronze ETL above in Pre-Hook
-- Silver ETL below 

with 
    base as (select * from {{ source('raw_robinhood', 'dividends') }}),

    all_stg_data as (
        select distinct
            value['id']::string as robinhood_dividend_id,
            value['drip_order_id']::string as robinhood_drip_order_id,
            value['tckr']::string as ticker,
            value['amount']::number(38,2) as amount,
            value['paid_at']::datetime as paid_at,
            value['payable_date']::datetime as payable_date,
            value['rate']::number(38,2) as amount_per_share,
            value['position']::number(38,2) as shares_owned,
            value['drip_enabled']::boolean as drip_enabled,
            value['record_date']::date as record_date,
            value['state']::string as status,
            log_ingest_date,
            row_number() over(partition by robinhood_dividend_id order by log_ingest_date desc) as r
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

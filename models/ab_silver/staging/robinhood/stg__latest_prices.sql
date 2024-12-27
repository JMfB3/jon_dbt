{{ config(
    materialized='view',
    tags=['stg'],
    query_tag="DBT__STAGING__ROBINHOOD",
    pre_hook=[
        "create stage if not exists {{ env_var('DBT_DB') }}.jons_stages.rh__latest_prices 
            url = {{ env_var('DBT_RH_STAGE') }}latest_prices/'
            credentials = (aws_key_id = '{{ env_var('DBT_AWS_KEY') }}', aws_secret_key = '{{ env_var('DBT_AWS_SECRET') }}');
        copy into {{ source('raw_robinhood', 'latest_prices') }} 
        from (
            select 
                t.$1 as data,
                METADATA$FILE_ROW_NUMBER row_num,
                METADATA$FILENAME as filename,
                METADATA$START_SCAN_TIME as log_ingest_date,
                METADATA$FILE_LAST_MODIFIED as file_last_modified_date,
            from @{{ env_var('DBT_DB') }}.jons_stages.rh__latest_prices t)
        file_format = (type = json)
        on_error = 'SKIP_FILE' "]
    )
}}

-- Bronze ETL above in Pre-Hook
-- Silver ETL below 

with 
    base as (select * from {{ source('raw_robinhood', 'latest_prices') }}),

    all_stg_data as (
        select distinct
            value['ticker']::string as ticker,
            value['latest_price'][0]::number(38, 2) as latest_price,
            log_ingest_date,
            row_number() over(partition by ticker order by log_ingest_date desc) as r
        from base,
        lateral flatten(input => parse_json(base.data))
    ),

    final as (
        select a.* exclude(r)
        from all_stg_data a
        where a.r = 1
    )

select *
from final 

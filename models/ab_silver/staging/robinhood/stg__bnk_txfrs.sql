{{ config(
    materialized='view',
    tags=['stg'],
    query_tag="DBT__STAGING__ROBINHOOD",
    pre_hook=[
        "create stage if not exists {{ env_var('DBT_DB') }}.jons_stages.rh__bnk_txfrs 
            url = {{ env_var('DBT_RH_STAGE') }}bnk_txfrs/'
            credentials = (aws_key_id = '{{ env_var('DBT_AWS_KEY') }}', aws_secret_key = '{{ env_var('DBT_AWS_SECRET') }}');
        copy into {{ source('raw_robinhood', 'bnk_txfrs') }} 
        from (
            select 
                t.$1 as data,
                METADATA$FILE_ROW_NUMBER row_num,
                METADATA$FILENAME as filename,
                METADATA$START_SCAN_TIME as log_ingest_date,
                METADATA$FILE_LAST_MODIFIED as file_last_modified_date,
            from @{{ env_var('DBT_DB') }}.jons_stages.rh__bnk_txfrs t)
        file_format = (type = json)
        on_error = 'SKIP_FILE' "]
    )
}}

-- Bronze ETL above in Pre-Hook
-- Silver ETL below 

with 
    base as (select * from {{ source('raw_robinhood', 'bnk_txfrs') }}),

    all_stg_data as (
        select distinct
            value['id']::string as robinhood_bnk_txfr_id,
            value['amount']::number(38,2) as amount,
            value['direction']::string as direction,
            value['status']::string as status,
            value['fees']::number(38,2) as fees,
            value['scheduled']::boolean as scheduled,
            value['created_at']::date as created_at,
            value['updated_at']::date as updated_at,
            log_ingest_date,
            row_number() over(partition by robinhood_bnk_txfr_id order by log_ingest_date desc) as r
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

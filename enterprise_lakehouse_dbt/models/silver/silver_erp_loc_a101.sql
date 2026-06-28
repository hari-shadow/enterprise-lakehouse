{{ config(
    materialized='incremental',
    unique_key='cid'
) }}

with max_ts as (
    {% if is_incremental() %}
        select max(_airbyte_extracted_at) as value from {{ this }}
    {% else %}
        select cast('1900-01-01' as timestamp) as value
    {% endif %}
),

source as (
    select s.*
    from {{ source('bronze', 'erp_loc_a101') }} s
    cross join max_ts
    where s.cid is not null
    and s._airbyte_extracted_at > max_ts.value
),

cleaned as (
    select
        replace(cid, '-', '')                                       as cid,
        case
            when trim(cntry) = 'DE'           then 'Germany'
            when trim(cntry) in ('USA', 'US') then 'United States'
            when trim(cntry) is null
              or trim(cntry) = ''             then 'n/a'
            else trim(cntry)
        end                                                         as cntry,
        _airbyte_extracted_at,
        current_timestamp()                                         as dwh_create_date
    from source
)

select * from cleaned
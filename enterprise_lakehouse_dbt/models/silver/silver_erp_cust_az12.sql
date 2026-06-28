{{ config(
    materialized='incremental',
    unique_key='cst_id'
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
    from {{ source('bronze', 'erp_cust_az12') }} s
    cross join max_ts
    where s.cid is not null
    and s._airbyte_extracted_at > max_ts.value
),

cleaned as (
    select
        case
            when cid like 'NAS%' then substring(cid, 4, len(cid))
            else cid
        end                                                         as cst_id,
        case
            when bdate >= current_date() then null
            else bdate
        end                                                         as birth_date,
        case
            when lower(trim(gen)) in ('f', 'female') then 'Female'
            when lower(trim(gen)) in ('m', 'male')   then 'Male'
            else 'n/a'
        end                                                         as gender,
        _airbyte_extracted_at,
        current_timestamp()                                         as dwh_create_date
    from source
)

select * from cleaned
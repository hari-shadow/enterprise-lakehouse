{{ config(
    materialized='incremental',
    unique_key='prd_id' 
) }}

with max_ts as (
    {% if is_incremental() %}
        select max(_airbyte_extracted_at) as value from {{ this }}
    {% else %}
        select try_cast('1900-01-01' as timestamp) as value
    {% endif %}
),

source as (
    select s.*
    from {{ source('bronze', 'product_info') }} s
    cross join max_ts
    where s.prd_id is not null and s.prd_id != ''
    and s._airbyte_extracted_at > max_ts.value
),

cleaned as (
    select
        prd_id,
        replace(substring(prd_key, 1, 5), '-', '_')                as cat_id,
        substring(prd_key, 7, len(prd_key))                        as prd_key,
        prd_nm                                                     as prd_name,
        coalesce(try_cast(prd_cost as bigint), 0)                     as prd_cost,
        case upper(trim(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'T' then 'Touring'
            when 'S' then 'Other Sales'
            else 'n/a'
        end                                                         as prd_line,
        prd_start_dt                                                as prd_start_date,
        try_cast(
            lead(prd_start_dt) over(
                partition by prd_key order by prd_id
            ) - interval 1 day
        as date)                                                    as prd_end_date,
        _airbyte_extracted_at,
        current_timestamp()                                         as dwh_create_date
    from source
)

select * from cleaned
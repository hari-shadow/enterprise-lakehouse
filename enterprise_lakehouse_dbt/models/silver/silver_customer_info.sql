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
    from {{ source('bronze', 'customer_info') }} s
    cross join max_ts
    where s.cst_id is not null
    and s._airbyte_extracted_at > max_ts.value
),

deduplicated as (
    select
        *,
        row_number() over(partition by cst_id order by cst_create_date desc) as rank
    from source
),

cleaned as (
    select
        cst_id,
        cst_key,
        trim(cst_firstname)                                         as cst_firstname,
        trim(cst_lastname)                                          as cst_lastname,
        case upper(trim(cst_marital_status))
            when 'M' then 'Married'
            when 'S' then 'Single'
            else 'n/a'
        end                                                         as cst_marital_status,
        case upper(trim(cst_gndr))
            when 'M' then 'Male'
            when 'F' then 'Female'
            else 'n/a'
        end                                                         as cst_gender,
        cst_create_date,
        _airbyte_extracted_at,
        current_timestamp()                                         as dwh_create_date
    from deduplicated
    where rank = 1
)

select * from cleaned
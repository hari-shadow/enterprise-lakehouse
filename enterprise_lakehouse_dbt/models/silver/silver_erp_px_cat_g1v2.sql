{{ config(
    materialized='incremental',
    unique_key='id'
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
    from {{ source('bronze', 'erp_px_cat_g1v2') }} s
    cross join max_ts
    where s.id is not null
    and s._airbyte_extracted_at > max_ts.value
),

cleaned as (
    select
        id,
        cat,
        subcat,
        maintenance,
        _airbyte_extracted_at,
        current_timestamp()                                         as dwh_create_date
    from source
)

select * from cleaned
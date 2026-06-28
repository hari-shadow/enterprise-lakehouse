{{ config(
    materialized='incremental',
    unique_key='sales_order_id'
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
    from {{ source('bronze', 'sales_info') }} s
    cross join max_ts
    where s.sls_ord_num is not null
    and s._airbyte_extracted_at > max_ts.value
),

cleaned as (
    select
        sls_ord_num                                                         as sales_order_id,
        sls_prd_key                                                         as sales_prd_key,
        try_cast(sls_cust_id as bigint)                                     as sales_cst_id,
        case
            when try_cast(sls_order_dt as bigint) <= 0
              or len(try_cast(sls_order_dt as string)) < 8 then null
            else to_date(try_cast(sls_order_dt as string), 'yyyyMMdd')
        end                                                                 as sales_order_date,
        case
            when try_cast(sls_ship_dt as bigint) <= 0
              or len(try_cast(sls_ship_dt as string)) < 8 then null
            else to_date(try_cast(sls_ship_dt as string), 'yyyyMMdd')
        end                                                                 as sales_ship_date,
        case
            when try_cast(sls_due_dt as bigint) <= 0
              or len(try_cast(sls_due_dt as string)) < 8 then null
            else to_date(try_cast(sls_due_dt as string), 'yyyyMMdd')
        end                                                                 as sales_due_date,
        case
            when try_cast(sls_sales as bigint) is null
              or try_cast(sls_sales as bigint) <= 0
              or try_cast(sls_sales as bigint) != try_cast(sls_quantity as bigint) * try_cast(sls_price as bigint)
            then try_cast(sls_quantity as bigint) * abs(try_cast(sls_price as bigint))
            else try_cast(sls_sales as bigint)
        end                                                                 as sales_amount,
        try_cast(sls_quantity as bigint)                                        as sales_qty,
        case
            when try_cast(sls_price as bigint) is null
              or try_cast(sls_price as bigint) <= 0
            then try_cast(sls_sales as bigint) / nullif(try_cast(sls_quantity as bigint), 0)
            else try_cast(sls_price as bigint)
        end                                                                 as sales_unit_price,
        _airbyte_extracted_at,
        current_timestamp()                                                 as dwh_create_date
    from source
)

select * from cleaned
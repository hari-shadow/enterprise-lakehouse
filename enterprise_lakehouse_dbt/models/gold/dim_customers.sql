{{ config(
    materialized='table',
    schema='gold'
) }}

with customer_info as (
    select * from {{ ref('silver_customer_info') }}
),

erp_loc as (
    select * from {{ ref('silver_erp_loc_a101') }}
),

erp_cust as (
    select * from {{ ref('silver_erp_cust_az12') }}
),

final as (
    select
        row_number() over(order by ci.cst_id)       as customer_key,
        ci.cst_id                                   as customer_id,
        ci.cst_key                                  as customer_number,
        ci.cst_firstname                            as customer_firstname,
        ci.cst_lastname                             as customer_lastname,
        concat(ci.cst_firstname, ' ', ci.cst_lastname) as customer_fullname,
        ci.cst_marital_status                       as customer_marital_status,
        ci.cst_gender                               as customer_gender,
        el.cntry                                    as customer_country,
        ec.birth_date                               as customer_birth_date,
        ci.cst_create_date                          as customer_created_on
    from customer_info ci
    left join erp_loc el
        on ci.cst_key = el.cid
    left join erp_cust ec
        on ci.cst_key = ec.cst_id
)

select * from final
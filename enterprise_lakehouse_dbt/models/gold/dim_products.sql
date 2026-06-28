{{ config(
    materialized='table',
    schema='gold'
) }}

with product_info as (
    select * from {{ ref('silver_product_info') }}
),

erp_cat as (
    select * from {{ ref('silver_erp_px_cat_g1v2') }}
),

final as (
    select
        row_number() over(order by pi.prd_start_date, pi.prd_id) as product_key,
        pi.prd_id                                   as product_id,
        pi.prd_name                                 as product_name,
        pi.prd_key                                  as product_number,
        pi.prd_cost                                 as product_cost,
        pi.prd_line                                 as product_line,
        pi.cat_id                                   as category_id,
        ec.cat                                      as product_category_name,
        ec.subcat                                   as product_subcategory_name,
        ec.maintenance                              as product_maintenance,
        pi.prd_start_date                           as product_start_date
    from product_info pi
    left join erp_cat ec
        on pi.cat_id = ec.id
    where pi.prd_end_date is null
)

select * from final
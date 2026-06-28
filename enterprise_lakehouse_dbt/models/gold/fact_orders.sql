{{ config(
    materialized='table',
    schema='gold'
) }}

with sales_info as (
    select * from {{ ref('silver_sales_info') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        row_number() over(order by si.sales_order_date, si.sales_order_id) as order_key,
        si.sales_order_id                           as order_id,
        dc.customer_key                             as customer_key,
        dp.product_key                              as product_key,
        si.sales_order_date                         as order_date,
        si.sales_ship_date                          as ship_date,
        si.sales_due_date                           as due_date,
        si.sales_amount                             as total_amount,
        si.sales_qty                                as order_qty,
        si.sales_unit_price                         as product_unit_price
    from sales_info si
    left join dim_customers dc
        on si.sales_cst_id = dc.customer_id
    left join dim_products dp
        on si.sales_prd_key = dp.product_number
)

select * from final
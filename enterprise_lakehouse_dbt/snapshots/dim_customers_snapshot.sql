{% snapshot dim_customers_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_marital_status',
            'customer_gender',
            'customer_country'
        ],
        invalidate_hard_deletes=True
    )
}}

select * from {{ ref('dim_customers') }}

{% endsnapshot %}
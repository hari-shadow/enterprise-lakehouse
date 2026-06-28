SHOW TABLES IN enterprise_lakehouse.bronze;

LIST 's3://enterprise-lakehouse-bronze/raw';

LIST 's3://enterprise-lakehouse-bronze/raw/customer_info/';

SELECT * FROM enterprise_lakehouse.bronze.customer_info LIMIT 5;

SELECT * FROM enterprise_lakehouse.gold.fact_orders LIMIT 5;

SELECT sales_order_date, sales_ship_date, sales_due_date 
FROM enterprise_lakehouse.silver.silver_sales_info 
LIMIT 5;

SELECT sales_cst_id, sales_prd_key, sales_order_date
FROM enterprise_lakehouse.silver.silver_sales_info
LIMIT 5;

SELECT * FROM enterprise_lakehouse.gold.fact_orders 
WHERE order_date IS NOT NULL
LIMIT 5;
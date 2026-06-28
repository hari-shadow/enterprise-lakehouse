CREATE EXTERNAL TABLE enterprise_lakehouse.bronze.customer_info
USING PARQUET
LOCATION 's3://enterprise-lakehouse-bronze/raw/customer_info/';

CREATE EXTERNAL TABLE enterprise_lakehouse.bronze.product_info
USING PARQUET
LOCATION 's3://enterprise-lakehouse-bronze/raw/product_info/';

CREATE EXTERNAL TABLE enterprise_lakehouse.bronze.sales_info
USING PARQUET
LOCATION 's3://enterprise-lakehouse-bronze/raw/sales_info/';

CREATE EXTERNAL TABLE enterprise_lakehouse.bronze.erp_cust_az12
USING PARQUET
LOCATION 's3://enterprise-lakehouse-bronze/raw/erp_cust_az12/';

CREATE EXTERNAL TABLE enterprise_lakehouse.bronze.erp_loc_a101
USING PARQUET
LOCATION 's3://enterprise-lakehouse-bronze/raw/erp_loc_a101/';

CREATE EXTERNAL TABLE enterprise_lakehouse.bronze.erp_px_cat_g1v2
USING PARQUET
LOCATION 's3://enterprise-lakehouse-bronze/raw/erp_px_cat_g1v2/';
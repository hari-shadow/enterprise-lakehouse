SHOW TABLES IN enterprise_lakehouse.bronze;

LIST 's3://enterprise-lakehouse-bronze/raw';

LIST 's3://enterprise-lakehouse-bronze/raw/customer_info/';

SELECT * FROM enterprise_lakehouse.bronze.customer_info LIMIT 5;
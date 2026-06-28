SELECT pg_create_logical_replication_slot('airbyte_crm_slot', 'pgoutput');

CREATE PUBLICATION airbyte_crm_pub FOR TABLE 
    customer_info, 
    product_info, 
    sales_info;


SELECT pg_create_logical_replication_slot('airbyte_erp_slot', 'pgoutput');

CREATE PUBLICATION airbyte_erp_pub FOR TABLE 
    erp_cust_az12, 
    erp_loc_a101, 
    erp_px_cat_g1v2;
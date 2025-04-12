/*
===============================================================================
Initializing file : Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY ... FROM` command to load data from csv Files to bronze tables.
===============================================================================
*/


TRUNCATE TABLE crm_cust_info;	
BEGIN;
LOCK TABLE crm_cust_info IN EXCLUSIVE MODE;
COPY crm_cust_info FROM 'C:/temp/dwh_project/dataset/source_crm/cust_info.csv'
WITH CSV HEADER DELIMITER ',';
COMMIT;

TRUNCATE TABLE crm_prd_info;
BEGIN;
LOCK TABLE crm_prd_info IN EXCLUSIVE MODE;
COPY crm_prd_info FROM 'C:/temp/dwh_project/dataset/source_crm/prd_info.csv'
WITH CSV HEADER DELIMITER ',';
COMMIT;
	
TRUNCATE TABLE crm_sales_details;
BEGIN;
LOCK TABLE crm_sales_details IN EXCLUSIVE MODE;
COPY crm_sales_details FROM 'C:/temp/dwh_project/dataset/source_crm/sales_details.csv'
WITH CSV HEADER DELIMITER ',';
COMMIT;
	
TRUNCATE TABLE erp_cust_az12;	
BEGIN;
LOCK TABLE erp_cust_az12 IN EXCLUSIVE MODE;
COPY erp_cust_az12 FROM 'C:/temp/dwh_project/dataset/source_erp/CUST_AZ12.csv'
WITH CSV HEADER DELIMITER ',';
COMMIT;
	
TRUNCATE TABLE erp_loc_a101;
BEGIN;
LOCK TABLE erp_loc_a101 IN EXCLUSIVE MODE;
COPY erp_loc_a101 FROM 'C:/temp/dwh_project/dataset/source_erp/LOC_A101.csv'
WITH CSV HEADER DELIMITER ',';
COMMIT;
	
TRUNCATE TABLE erp_px_cat_g1v2;	
BEGIN;
LOCK TABLE erp_px_cat_g1v2 IN EXCLUSIVE MODE;
COPY erp_px_cat_g1v2 FROM 'C:/temp/dwh_project/dataset/source_erp/PX_CAT_G1V2.csv'
WITH CSV HEADER DELIMITER ',';
COMMIT;	
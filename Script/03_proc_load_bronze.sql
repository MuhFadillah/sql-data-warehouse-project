/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY ... FROM` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql 
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := now();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

	BEGIN

	    RAISE NOTICE '================================================';
	    RAISE NOTICE 'Loading CRM Tables';
	    RAISE NOTICE '================================================';

	    -- **CRM_CUST_INFO**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	    TRUNCATE TABLE crm_cust_info;
	
	    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	    LOCK TABLE crm_cust_info IN EXCLUSIVE MODE;
	    COPY crm_cust_info 
	    FROM 'C:/temp/dwh_project/dataset/source_crm/cust_info.csv'
	    WITH CSV HEADER DELIMITER ',';
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for crm_cust_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';
	
	    -- **CRM_PRD_INFO**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	    TRUNCATE TABLE crm_prd_info;
	
	    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	    LOCK TABLE crm_prd_info IN EXCLUSIVE MODE;
	    COPY crm_prd_info 
	    FROM 'C:/temp/dwh_project/dataset/source_crm/prd_info.csv'
	    WITH CSV HEADER DELIMITER ',';
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for crm_prd_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';
	
	    -- **CRM_SALES_DETAILS**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	    TRUNCATE TABLE crm_sales_details;
	
	    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		LOCK TABLE crm_sales_details IN EXCLUSIVE MODE;
		COPY crm_sales_details 
		FROM 'C:/temp/dwh_project/dataset/source_crm/sales_details.csv'
		WITH CSV HEADER DELIMITER ',';
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for crm_sales_details: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    RAISE NOTICE '================================================';
	    RAISE NOTICE 'Loading ERP Tables';
	    RAISE NOTICE '================================================';
	
	    -- **ERP_CUST_AZ12**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	    TRUNCATE TABLE erp_cust_az12;
	
	    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		LOCK TABLE erp_cust_az12 IN EXCLUSIVE MODE;
		COPY erp_cust_az12 
		FROM 'C:/temp/dwh_project/dataset/source_erp/CUST_AZ12.csv'
		WITH CSV HEADER DELIMITER ',';
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for erp_cust_az12: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';
	
	    -- **ERP_LOC_A101**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	    TRUNCATE TABLE erp_loc_a101;
	
	    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		LOCK TABLE erp_loc_a101 IN EXCLUSIVE MODE;
		COPY erp_loc_a101 
		FROM 'C:/temp/dwh_project/dataset/source_erp/LOC_A101.csv'
		WITH CSV HEADER DELIMITER ',';
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for erp_loc_a101: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';
	
	    -- **ERP_PX_CAT_G1V2**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	    TRUNCATE TABLE erp_px_cat_g1v2;
	
	    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		LOCK TABLE erp_px_cat_g1v2 IN EXCLUSIVE MODE;
		COPY erp_px_cat_g1v2 
		FROM 'C:/temp/dwh_project/dataset/source_erp/PX_CAT_G1V2.csv'
		WITH CSV HEADER DELIMITER ',';
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for erp_px_cat_g1v2: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';
	
	    -- **Total Waktu Proses**
	    batch_end_time := now();
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Loading Bronze Layer is Completed';
		RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
		RAISE NOTICE '==========================================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error SQLSTATE: %', SQLSTATE;
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE '==========================================';
    END;

END;
$$;

DROP PROCEDURE IF EXISTS bronze.load_bronze;

CALL bronze.load_bronze();
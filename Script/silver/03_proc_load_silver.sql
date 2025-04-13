/*
===============================================================================
Stored Procedure: Load Silver Layer (bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
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
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

	BEGIN
	    RAISE NOTICE '================================================';
	    RAISE NOTICE 'Loading CRM Tables';
	    RAISE NOTICE '================================================';

	    -- **CRM_CUST_INFO**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info ';
		TRUNCATE TABLE silver.crm_cust_info;

	    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info ';
		INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr,
		cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		trim(cst_firstname) AS cst_firstname,
		trim(cst_lastname) AS cst_lastname,
			CASE 
				WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
				WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, 
			CASE 
				WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
				WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,  
		cst_create_date
		FROM 
		(
			SELECT *,
			ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
			FROM bronze.crm_cust_info cci 
			WHERE cst_id IS NOT NULL 
		) m
		WHERE flag_last = 1;
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for crm_cust_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    -- **CRM_PRD_INFO**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

	    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
		prd_id ,
		REPLACE (substring(prd_key,1,5),'-','_' ) AS cat_id,
		substring (prd_key,7, length(prd_key)) AS prd_key ,
		prd_nm,
		coalesce (prd_cost,0) as prd_cost,
			CASE 
				WHEN upper (trim(prd_line)) = 'M' THEN 'Mountain'
				WHEN upper (trim(prd_line)) = 'R' THEN 'Road'
				WHEN upper (trim(prd_line)) = 'S' THEN 'Other Sales'
				WHEN upper (trim(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, 
		CAST (prd_start_dt AS date) AS prd_start_dt ,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt 
		FROM bronze.crm_prd_info cpi ;
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for crm_prd_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    -- **CRM_SALES_DETAILS**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details ;

	    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
			CASE 
			    WHEN sls_order_dt = 0 OR LENGTH(cast(sls_order_dt AS varchar)) != 8 THEN NULL
			    ELSE cast(CAST (sls_order_dt AS varchar) AS date)
			END AS sls_order_dt,
			CASE 
			    WHEN sls_ship_dt = 0 OR LENGTH(cast(sls_ship_dt  AS varchar)) != 8 THEN NULL
			    ELSE cast(CAST (sls_ship_dt  AS varchar) AS date)
			END AS sls_ship_dt ,
			CASE 
			    WHEN sls_due_dt = 0 OR LENGTH(cast(sls_due_dt  AS varchar)) != 8 THEN NULL
			    ELSE cast(CAST (sls_due_dt  AS varchar) AS date)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales ISNULL OR sls_sales <= 0 OR sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price)
				ELSE sls_sales 
			END AS sls_sales, 
			sls_quantity ,
			CASE 
				WHEN sls_price ISNULL OR sls_price <= 0 THEN sls_sales  / NULLIF (sls_quantity,0)
				ELSE sls_price 
			END AS sls_price 
		FROM bronze.crm_sales_details csd ;
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for crm_sales_details: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    RAISE NOTICE '================================================';
	    RAISE NOTICE 'Loading ERP Tables';
	    RAISE NOTICE '================================================';

	    -- **ERP_CUST_AZ12**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12 ;

	    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid)) 
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > now() THEN NULL
				ELSE bdate
			END AS bdate, 
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen 
		FROM bronze.erp_cust_az12;
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for erp_cust_az12: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    -- **ERP_LOC_A101**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

	    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101 ';
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		SELECT 
		REPLACE(cid, '-', '') AS cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry ISNULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry 
		FROM bronze.erp_loc_a101;
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for erp_loc_a101: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    -- **ERP_PX_CAT_G1V2**
	    start_time := now();
	    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

	    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
	    end_time := now();
	    RAISE NOTICE '>> Load Duration for erp_px_cat_g1v2: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	    RAISE NOTICE '>> -------------------------------------------';

	    -- **Total Waktu Proses**
	    batch_end_time := now();
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Loading Silver Layer is Completed';
		RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
		RAISE NOTICE '==========================================';
	EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'Error SQLSTATE: %', SQLSTATE;
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE '==========================================';
	END;
END;
$$;

DROP PROCEDURE IF EXISTS silver.load_silver;

CALL silver.load_silver();
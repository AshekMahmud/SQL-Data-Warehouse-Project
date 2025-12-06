
/*
------------------------------------------------------------
Stored Procedure: Load second_layer (first_layer -> second_layer)
------------------------------------------------------------
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load)
	process to populate the 'second_layer' schema tables from the
	'first_layer' schema.
	Actions Performed:
	- Truncats second_layer tables.
	- Inserts transformed and cleansed data from first_layer into second_layer
	tables.

Parameters:
	None.
	This stored procedure does ot accept any parameters or return any values.

Usage Example:
	EXEC second_layer.load_second_layer;

Use the above example to load the second_layer tables using the Procedure
'second_layer.load_second_layer.
------------------------------------------------------------
*/

CREATE OR ALTER PROCEDURE second_layer.load_second_layer AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading second_layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';
		
		-- Loading second_layer.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: second_layer.crm_cust_info';
		TRUNCATE TABLE second_layer.crm_cust_info;
		PRINT '>> Inserting Data Into: second_layer.crm_cust_info';
		INSERT INTO second_layer.crm_cust_info(
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM(
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM first_layer.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT 'Completed Data Insertion into: second_layer.crm_cust_info'
		PRINT '--------------------------------------------';
		
		-------------------------------------------------------------------------

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: second_layer.crm_prd_info'
		TRUNCATE TABLE second_layer.crm_prd_info;
		PRINT '>> Inserting Data Into: second_layer.crm_prd_info'
		INSERT INTO second_layer.crm_prd_info(
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
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM first_layer.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT 'Completed Data Insertion into: second_layer.crm_prd_info';
		PRINT '--------------------------------------------';

		-------------------------------------------------------------------------
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: second_layer.crm_sales_details'
		TRUNCATE TABLE second_layer.crm_sales_details;
		PRINT '>> Inserting Data Into: second_layer.crm_sales_details'
		INSERT INTO second_layer.crm_sales_details(
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
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
		FROM first_layer.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT 'Completed Data Insertion into: second_layer.crm_sales_details';
		PRINT '--------------------------------------------';

		-------------------------------------------------------------------------

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		-------------------------------------------------------------------------

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: second_layer.erp_cust'
		TRUNCATE TABLE second_layer.erp_cust;
		PRINT '>> Inserting Data Into: second_layer.erp_cust'
		INSERT INTO second_layer.erp_cust(
			CID,
			BDATE,
			GEN
		)

		SELECT 
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			ELSE CID
		END AS CID,
		CASE WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE,
		CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
			WHEN UPPER(TRIM(GEN)) IN ('M','Male') THEN 'Male'
			ELSE 'n/a'
		END AS GEN
		FROM first_layer.erp_cust;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT 'Completed Data Insertion into: second_layer.erp_cust';
		PRINT '--------------------------------------------';

		-------------------------------------------------------------------------

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: second_layer.erp_loc'
		TRUNCATE TABLE second_layer.erp_loc;
		PRINT '>> Inserting Data Into: second_layer.erp_loc'
		INSERT INTO second_layer.erp_loc(
			CID,
			CNTRY
		)

		SELECT 
		REPLACE(CID, '-', '') AS CID,
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			ELSE TRIM(CNTRY)
		END AS CNTRY
		FROM first_layer.erp_loc;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT 'Completed Data Insertion into: second_layer.erp_loc';
		PRINT '--------------------------------------------';

		-------------------------------------------------------------------------

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: second_layer.erp_px_cat'
		TRUNCATE TABLE second_layer.erp_px_cat;
		PRINT '>> Inserting Data Into: second_layer.erp_px_cat'

		INSERT INTO second_layer.erp_px_cat(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)

		SELECT 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM first_layer.erp_px_cat;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT 'Completed Data Insertion into: second_layer.erp_px_cat';
		PRINT '--------------------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT '============================================'
		PRINT '- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '============================================'
	END TRY
	BEGIN CATCH
		PRINT '============================================'
		PRINT 'ERROR OCCURED DURING second_layer'
		PRINT 'Error Massage: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH
END

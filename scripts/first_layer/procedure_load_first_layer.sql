/*
--------------------------------------------------
Stored Procedure: Load first_layer (Source -> first_layer)
--------------------------------------------------
Script Purpose:
	This stored procedure loads data into the 'first_layer' schema for external CSV files.
	It performs the following actions:
	- Truncates the first_layer tables before loading data.
	- Uses the 'BULK INSERT' method to load data from csv files into the first_layer tables.

Parameters:
	None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC first_layer.load_bronze;

Note:
	Created PROCEDURE to execute the entire command at once.
	Use it like the presented example.
*/



CREATE OR ALTER PROCEDURE first_layer.load_first_layer AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '-------------------------------------------';
		PRINT 'LOADING first_layer'
		PRINT '-------------------------------------------';
		PRINT 'LOADING CRM DATA';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING first_layer.crm_cust_info';
		TRUNCATE TABLE first_layer.crm_cust_info;

		PRINT '>>> INSERTING DATA INTO: first_layer.crm_cust_info';
		BULK INSERT first_layer.crm_cust_info
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '-------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING first_layer.crm_prd_info';
		TRUNCATE TABLE first_layer.crm_prd_info;

		PRINT '>>> INSERTING DATA INTO: first_layer.crm_prd_info';
		BULK INSERT first_layer.crm_prd_info
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '-------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING first_layer.crm_sales_details';
		TRUNCATE TABLE first_layer.crm_sales_details;

		PRINT '>>> INSERTING DATA INTO: first_layer.crm_sales_details';
		BULK INSERT first_layer.crm_sales_details
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_detailS.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '-------------------------------------------';


		PRINT '-------------------------------------------';
		PRINT 'LOADING ERP DATA';
		PRINT '-------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING first_layer.erp_cust';
		TRUNCATE TABLE first_layer.erp_cust;

		PRINT '>>> INSERTING DATA INTO: first_layer.erp_cust';
		BULK INSERT first_layer.erp_cust
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '-------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING first_layer.erp_loc';
		TRUNCATE TABLE first_layer.erp_loc;

		PRINT '>>> INSERTING DATA INTO: first_layer.erp_loc';
		BULK INSERT first_layer.erp_loc
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '-------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING first_layer.erp_px_cat';
		TRUNCATE TABLE first_layer.erp_px_cat;

		PRINT '>>> INSERTING DATA INTO: first_layer.erp_px_cat';
		BULK INSERT first_layer.erp_px_cat
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' ' + 'seconds';
		PRINT '-------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '-------------------------------------------'
		PRINT 'LOADING first_layer IS COMPLETED'
		PRINT 'TOTAL LOADING DURATION: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ' + 'seconds'
		PRINT '-------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '-------------------------------------------';
		PRINT 'ERROR OCCURED DURING LOADING	fist_layer';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------';
	END CATCH
END

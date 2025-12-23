/*
-------------------------------------------------------------------------------
Quality Checks
-------------------------------------------------------------------------------
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'second_layer' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading second_layer Layer.
    - Investigate and resolve any discrepancies found during the checks.
-------------------------------------------------------------------------------
*/

-------------------------------------------------------------------------------
-- Checking 'second_layer.crm_cust_info'
-----------------------------------------------------------------------
-- Check for NULLs or Duplicates in Primary Key


USE Data_Warehouse;

SELECT 
    cst_id,
    COUNT(*) 
FROM second_layer.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces

SELECT 
    cst_key 
FROM second_layer.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM second_layer.crm_cust_info;

-----------------------------------------------------------------------
-- Checking 'second_layer.crm_prd_info'
-----------------------------------------------------------------------
-- Check for NULLs or Duplicates in Primary Key

SELECT 
    prd_id,
    COUNT(*) 
FROM second_layer.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces

SELECT 
    prd_nm 
FROM second_layer.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost

SELECT 
    prd_cost 
FROM second_layer.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM second_layer.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)

SELECT * 
FROM second_layer.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-----------------------------------------------------------------------
-- Checking 'second_layer.crm_sales_details'
-----------------------------------------------------------------------
-- Check for Invalid Dates

SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM first_layer.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)

SELECT * 
FROM second_layer.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price

SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM second_layer.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-----------------------------------------------------------------------
-- Checking 'second_layer.erp_cust'
-----------------------------------------------------------------------
-- Identify Out-of-Range Dates

SELECT 
	DISTINCT bdate 
FROM second_layer.erp_cust
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM second_layer.erp_cust;

-----------------------------------------------------------------------
-- Checking 'second_layer.erp_loc'
-----------------------------------------------------------------------
-- Data Standardization & Consistency
SELECT 
	DISTINCT cntry 
FROM second_layer.erp_loc
ORDER BY cntry;

-----------------------------------------------------------------------
-- Checking 'second_layer.erp_px_cat'
-----------------------------------------------------------------------
-- Check for Unwanted Spaces

SELECT * 
FROM second_layer.erp_px_cat
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT 
	DISTINCT maintenance 
FROM second_layer.erp_px_cat;


-- Unwanted Spaces
SELECT *
FROM first_layer.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

--checking the unmatched data for product key
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM first_layer.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM second_layer.crm_prd_info);

--checking the unmatched data for product key
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM first_layer.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM second_layer.crm_cust_info);


-- checking invalid date
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt 
FROM first_layer.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;


SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt 
FROM first_layer.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101


SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM first_layer.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101


-- Checking for invalid Order Date

SELECT * 
FROM first_layer.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistancy between Sales, Quantity, and Price

SELECT 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM first_layer.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL 
OR sls_quantity IS NULL 
OR sls_price IS NULL
OR sls_sales <= 0 
OR sls_quantity <= 0 
OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- CHECKING

SELECT * 
FROM second_layer.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


SELECT 
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

SELECT * FROM second_layer.crm_sales_details;

---------------------------------------------------------
-- first_layer.erp_cust
---------------------------------------------------------


-- Identify out of Range Dates

SELECT 
BDATE,
CASE WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
END AS BDATE
FROM first_layer.erp_cust
WHERE BDATE < '1924-01-01' OR BDATE > GETDATE()

-- Data Standardization & Consistency

SELECT DISTINCT GEN
FROM first_layer.erp_cust

SELECT GEN,
CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
	WHEN UPPER(TRIM(GEN)) IN ('M','Male') THEN 'Male'
	ELSE 'n/a'
END AS GEN
FROM first_layer.erp_cust


-- Identify out of Range Dates

SELECT 
BDATE,
CASE WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
END AS BDATE
FROM second_layer.erp_cust
WHERE BDATE < '1924-01-01' OR BDATE > GETDATE()

-- Data Standardization & Consistency

SELECT DISTINCT GEN
FROM second_layer.erp_cust


SELECT *
FROM second_layer.erp_cust;


---------------------------------------------------------
-- first_layer.erp_loc
---------------------------------------------------------

-- Data Standardization & Consistency

SELECT DISTINCT
CNTRY AS OLD_CNTRY,
CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
	WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
	ELSE TRIM(CNTRY)
END AS CNTRY
FROM first_layer.erp_loc
ORDER BY CNTRY;


SELECT DISTINCT
CNTRY
FROM second_layer.erp_loc;

SELECT * FROM second_layer.erp_loc;


---------------------------------------------------------
-- first_layer.erp_px_cat
---------------------------------------------------------

SELECT 
ID,
CAT,
SUBCAT,
MAINTENANCE
FROM first_layer.erp_px_cat;

-- Unwanted Spaces
SELECT * FROM first_layer.erp_px_cat
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

-- Data Standardization & Consistency
SELECT DISTINCT CAT
FROM first_layer.erp_px_cat;

SELECT DISTINCT SUBCAT
FROM first_layer.erp_px_cat;

SELECT DISTINCT MAINTENANCE
FROM first_layer.erp_px_cat;

SELECT * FROM second_layer.erp_px_cat;

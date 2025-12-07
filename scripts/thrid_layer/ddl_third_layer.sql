
/*
-----------------------------------------------------
DDL Script: Create third_layer Views
-----------------------------------------------------
Script Purpose:
	This script creates views for the 'third_layer' in the data warehouse.
	The third_layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the second_layer
	to produce a clean, enriched, and business-ready dataset.

Usage:
	- These views can be queried directly for analytics and reporting.
-----------------------------------------------------
*/

-----------------------------------------------------
-- Create Dimension: third_layer.dim_customers
-----------------------------------------------------

IF OBJECT_ID('third_layer.dim_customers', 'V') IS NOT NULL
	DROP VIEW third_layer.dim_customers;
GO

CREATE VIEW third_layer.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY	cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- crm is the master data
		ELSE COALESCE(ca.GEN, 'n/a')
	END AS gender,
	ca.BDATE AS birthdate,
	ci.cst_create_date AS create_date	
FROM second_layer.crm_cust_info AS ci
LEFT JOIN second_layer.erp_cust AS ca
ON ci.cst_key = ca.CID
LEFT JOIN second_layer.erp_loc AS la
ON ci.cst_key = la.CID;
GO

-----------------------------------------------------
-- Create Dimension: third_layer.dim_products
-----------------------------------------------------

IF OBJECT_ID('third_layer.dim_products', 'V') IS NOT NULL
	DROP VIEW third_layer.dim_products;
GO

CREATE VIEW third_layer.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key, 
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS subcategory,
	pc.MAINTENANCE,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM second_layer.crm_prd_info pr
LEFT JOIN second_layer.erp_px_cat pc
ON pr.cat_id = pc.ID
WHERE prd_end_dt IS NULL; --	filterout all historical data
GO

-----------------------------------------------------
-- Create Fact Table: third_layer.fact_sales
-----------------------------------------------------

IF OBJECT_ID('third_layer.fact_sales', 'V') IS NOT NULL
	DROP VIEW third_layer.fact_sales;
GO

CREATE VIEW third_layer.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM second_layer.crm_sales_details AS sd
LEFT JOIN third_layer.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN third_layer.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id;
GO

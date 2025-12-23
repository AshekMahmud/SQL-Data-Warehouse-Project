/*
-----------------------------------------------------------------------
Quality Checks
-----------------------------------------------------------------------
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the third_layer Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
-----------------------------------------------------------------------
*/

-----------------------------------------------------------------------
-- Checking 'third_layer.dim_customers'
-----------------------------------------------------------------------
-- Check for Uniqueness of Customer Key in third_layer.dim_customers

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM third_layer.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-----------------------------------------------------------------------
-- Checking 'third_layer.product_key'
-----------------------------------------------------------------------
-- Check for Uniqueness of Product Key in third_layer.dim_products

SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM third_layer.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-----------------------------------------------------------------------
-- Checking 'third_layer.fact_sales'
-----------------------------------------------------------------------
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM third_layer.fact_sales f
LEFT JOIN third_layer.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN third_layer.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;  

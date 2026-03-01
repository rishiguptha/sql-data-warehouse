-- ============================================================
-- Script  : quality_checks_gold.sql
-- Layer   : Gold
-- Purpose : Validate the data quality of the Gold Layer star schema.
--           Checks include: uniqueness of surrogate keys in dimension
--           tables, and referential integrity between fact and dimension
--           tables. Run after load_gold.sql to confirm the analytical
--           model is sound. A clean gold layer should return 0 rows on
--           all checks.
-- Scope   : gold.dim_customers, gold.dim_products, gold.fact_sales
-- Usage   : Run after load_gold.sql. Investigate and resolve any
--           discrepancies found during the checks.
-- ============================================================


-- ============================================================
-- Quality Checks: gold.dim_customers
-- ============================================================
SELECT '>> [1/3] Starting quality checks for: gold.dim_customers' AS progress;

-- Check 1: Uniqueness of surrogate key (customer_key)
-- Expectation: No results
SELECT '   [1/3 | Check 1] Uniqueness of surrogate key (customer_key)' AS progress;
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

SELECT '>> [1/3] Completed quality checks for: gold.dim_customers' AS progress;


-- ============================================================
-- Quality Checks: gold.dim_products
-- ============================================================
SELECT '>> [2/3] Starting quality checks for: gold.dim_products' AS progress;

-- Check 1: Uniqueness of surrogate key (product_key)
-- Expectation: No results
SELECT '   [2/3 | Check 1] Uniqueness of surrogate key (product_key)' AS progress;
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

SELECT '>> [2/3] Completed quality checks for: gold.dim_products' AS progress;


-- ============================================================
-- Quality Checks: gold.fact_sales
-- ============================================================
SELECT '>> [3/3] Starting quality checks for: gold.fact_sales' AS progress;

-- Check 1: Referential integrity — fact rows must resolve to both dimensions
-- Expectation: No results
SELECT '   [3/3 | Check 1] Referential integrity — customer_key and product_key FKs' AS progress;
SELECT *
FROM gold.fact_sales  AS f
LEFT JOIN gold.dim_customers AS c
    ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
    ON f.product_key = p.product_key
WHERE c.customer_key IS NULL
   OR p.product_key  IS NULL;

SELECT '>> [3/3] Completed quality checks for: gold.fact_sales' AS progress;
SELECT '>> All gold layer quality checks complete.' AS progress;
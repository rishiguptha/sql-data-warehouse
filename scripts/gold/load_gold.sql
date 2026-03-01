-- ============================================================
-- Script  : load_gold.sql
-- Layer   : Gold
-- Purpose : Build the star schema (dim + fact tables) for analytical
--           reporting. Joins cleaned silver tables, adds surrogate keys,
--           and materializes the result as query-ready gold tables.
--           This script is idempotent — each table is dropped and
--           recreated on every run.
-- Source  : silver.crm_cust_info, silver.crm_prd_info,
--           silver.crm_sales_details, silver.erp_cust_az12,
--           silver.erp_loc_a101, silver.erp_px_cat_g1v2
-- Target  : gold.dim_customers, gold.dim_products, gold.fact_sales
-- ============================================================

.timer on

SELECT '====================' AS info;
SELECT '>> Starting gold layer load...' AS info;
SELECT '====================' AS info;

-- ============================================================
-- dim_customers
-- ============================================================

SELECT '>> Creating table: gold.dim_customers...' AS info;
DROP TABLE IF EXISTS gold.dim_customers;
CREATE TABLE gold.dim_customers AS 
    SELECT 
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
        ci.cst_id                           AS customer_id, 
        ci.cst_key                          AS customer_number, 
        ci.cst_firstname                    AS first_name, 
        ci.cst_lastname                     AS last_name, 
        loc.cntry                           AS country,
        ci.cst_marital_status               AS marital_status, 
        CASE WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr  -- CRM is master for gender
             ELSE COALESCE(ca.gen, 'n/a')
        END                                 AS gender,
        ca.bdate                            AS birth_date,
        ci.cst_create_date                  AS create_date
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS loc
        ON ci.cst_key = loc.cid
;
SELECT '>> gold.dim_customers created.' AS info;


-- ============================================================
-- dim_products
-- ============================================================

SELECT '>> Creating table: gold.dim_products...' AS info;
DROP TABLE IF EXISTS gold.dim_products;
CREATE TABLE gold.dim_products AS 
    SELECT 
        ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
        pn.prd_id       AS product_id, 
        pn.prd_key      AS product_number, 
        pn.prd_nm       AS product_name, 
        pn.cat_id       AS category_id, 
        pc.cat          AS category,
        pc.subcat       AS subcategory,
        pc.maintenance  AS maintenance,
        pn.prd_cost     AS product_cost, 
        pn.prd_line     AS product_line, 
        pn.prd_start_dt AS start_date
    FROM silver.crm_prd_info AS pn
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc
        ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL  -- filter out historical records
; 
SELECT '>> gold.dim_products created.' AS info;


-- ============================================================
-- fact_sales
-- ============================================================

SELECT '>> Creating table: gold.fact_sales...' AS info;
DROP TABLE IF EXISTS gold.fact_sales;
CREATE TABLE gold.fact_sales AS 
    SELECT 
        sd.sls_ord_num  AS order_number, 
        pr.product_key,
        c.customer_key,
        sd.sls_order_dt AS order_date, 
        sd.sls_ship_dt  AS shipping_date, 
        sd.sls_due_dt   AS due_date, 
        sd.sls_sales    AS sales_amount, 
        sd.sls_quantity AS quantity, 
        sd.sls_price    AS price 
    FROM silver.crm_sales_details AS sd
    LEFT JOIN gold.dim_products AS pr
        ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers AS c
        ON sd.sls_cust_id = c.customer_id
;
SELECT '>> gold.fact_sales created.' AS info;

SELECT '====================' AS info;
SELECT '>> Gold layer load complete.' AS info;
SELECT '====================' AS info;

.timer off
----- DATA LOADING INTO SILVER TABLES ------

SELECT '====================' AS info;
SELECT '>> Starting silver layer load...' AS info;
SELECT '====================' AS info;

-- ============================================================
-- SOURCE: CRM
-- ============================================================

-- Table: silver.crm_cust_info
-- Source: datasets/source_crm/cust_info.csv
-- Columns: cst_id, cst_key, cst_firstname, cst_lastname,
--           cst_marital_status, cst_gndr, cst_create_date
SELECT '>> Creating table: silver.crm_cust_info...' AS info;
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id              BIGINT,
    cst_key             VARCHAR,
    cst_firstname       VARCHAR,
    cst_lastname        VARCHAR,
    cst_marital_status  VARCHAR,
    cst_gndr            VARCHAR,
    cst_create_date     DATE,
    dwh_create_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: silver.crm_prd_info
-- Source: datasets/source_crm/prd_info.csv
-- Columns: prd_id, prd_key, prd_nm, prd_cost, prd_line,
--           prd_start_dt, prd_end_dt
SELECT '>> Creating table: silver.crm_prd_info...' AS info;
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id          BIGINT,
    cat_id          VARCHAR,
    prd_key         VARCHAR,
    prd_nm          VARCHAR,
    prd_cost        BIGINT,
    prd_line        VARCHAR,
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: silver.crm_sales_details
-- Source: datasets/source_crm/sales_details.csv
-- Columns: sls_ord_num, sls_prd_key, sls_cust_id,
--           sls_order_dt, sls_ship_dt, sls_due_dt,
--           sls_sales, sls_quantity, sls_price
SELECT '>> Creating table: silver.crm_sales_details...' AS info;
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     VARCHAR,
    sls_prd_key     VARCHAR,
    sls_cust_id     BIGINT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       BIGINT,
    sls_quantity    BIGINT,
    sls_price       BIGINT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SOURCE: ERP
-- ============================================================

-- Table: silver.erp_cust_az12
-- Source: datasets/source_erp/CUST_AZ12.csv
-- Columns: CID, BDATE, GEN
SELECT '>> Creating table: silver.erp_cust_az12...' AS info;
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR,
    bdate           DATE,
    gen             VARCHAR,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: silver.erp_loc_a101
-- Source: datasets/source_erp/LOC_A101.csv
-- Columns: CID, CNTRY
SELECT '>> Creating table: silver.erp_loc_a101...' AS info;
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR,
    cntry           VARCHAR,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: silver.erp_px_cat_g1v2
-- Source: datasets/source_erp/PX_CAT_G1V2.csv
-- Columns: ID, CAT, SUBCAT, MAINTENANCE
SELECT '>> Creating table: silver.erp_px_cat_g1v2...' AS info;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id              VARCHAR,
    cat             VARCHAR,
    subcat          VARCHAR,
    maintenance     BOOLEAN,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT '>> All silver tables created.' AS info;
SELECT '====================' AS info;

-- ============================================================
-- INSERT: silver.crm_cust_info
-- ============================================================

SELECT '>> Loading data into silver.crm_cust_info...' AS info;
SELECT '   >> Truncating silver.crm_cust_info...' AS info;
TRUNCATE TABLE silver.crm_cust_info;
SELECT '   >> Truncate complete. Inserting rows...' AS info;
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
WITH flag_last AS (
    SELECT * , 
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
            ) AS row_num 
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END                                             AS cst_marital_status,
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'F' THEN 'Female'
        WHEN 'M' THEN 'Male'
        ELSE 'n/a'
    END                                             AS cst_gndr,
    cst_create_date
FROM flag_last
WHERE row_num = 1;

SELECT '>> silver.crm_cust_info loaded.' AS info;

-- ============================================================
-- INSERT: silver.crm_prd_info
-- ============================================================

SELECT '>> Loading data into silver.crm_prd_info...' AS info;
SELECT '   >> Truncating silver.crm_prd_info...' AS info;
TRUNCATE TABLE silver.crm_prd_info;
SELECT '   >> Truncate complete. Inserting rows...' AS info;
INSERT INTO silver.crm_prd_info(
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')    AS cat_id,
    SUBSTRING(prd_key, 7)                          AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0)                          AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Sport'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END                                            AS prd_line,
    prd_start_dt::DATE,
    (LEAD(prd_start_dt) OVER (
        PARTITION BY prd_key
        ORDER BY prd_start_dt
    ) - INTERVAL '1 day')::DATE                    AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT '>> silver.crm_prd_info loaded.' AS info;

-- ============================================================
-- INSERT: silver.crm_sales_details
-- ============================================================

SELECT '>> Loading data into silver.crm_sales_details...' AS info;
SELECT '   >> Truncating silver.crm_sales_details...' AS info;
TRUNCATE TABLE silver.crm_sales_details;
SELECT '   >> Truncate complete. Inserting rows...' AS info;
INSERT INTO silver.crm_sales_details(
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
    CASE 
        WHEN sls_order_dt <= 0 OR LEN(sls_order_dt::VARCHAR) <> 8 THEN NULL
        ELSE STRPTIME(sls_order_dt::VARCHAR, '%Y%m%d')::DATE 
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt::VARCHAR) <> 8 THEN NULL
        ELSE STRPTIME(sls_ship_dt::VARCHAR, '%Y%m%d')::DATE 
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt <= 0 OR LEN(sls_due_dt::VARCHAR) <> 8 THEN NULL
        ELSE STRPTIME(sls_due_dt::VARCHAR, '%Y%m%d')::DATE 
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity, 
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) 
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

SELECT '>> silver.crm_sales_details loaded.' AS info;

-- ============================================================
-- INSERT: silver.erp_cust_az12
-- ============================================================

SELECT '>> Loading data into silver.erp_cust_az12...' AS info;
SELECT '   >> Truncating silver.erp_cust_az12...' AS info;
TRUNCATE TABLE silver.erp_cust_az12;
SELECT '   >> Truncate complete. Inserting rows...' AS info;
INSERT INTO silver.erp_cust_az12(
    cid, 
    bdate, 
    gen
)
SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
         ELSE cid
    END AS cid,
    CASE WHEN bdate > CURRENT_DATE THEN NULL
         ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
         ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;

SELECT '>> silver.erp_cust_az12 loaded.' AS info;

-- ============================================================
-- INSERT: silver.erp_loc_a101
-- ============================================================

SELECT '>> Loading data into silver.erp_loc_a101...' AS info;
SELECT '   >> Truncating silver.erp_loc_a101...' AS info;
TRUNCATE TABLE silver.erp_loc_a101;
SELECT '   >> Truncate complete. Inserting rows...' AS info;
INSERT INTO silver.erp_loc_a101(
    cid, cntry
)
SELECT 
    REPLACE(cid, '-', '') AS cid, 
    CASE WHEN TRIM(cntry) = 'DE'          THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         ELSE cntry 
    END AS cntry
FROM bronze.erp_loc_a101;

SELECT '>> silver.erp_loc_a101 loaded.' AS info;

-- ============================================================
-- INSERT: silver.erp_px_cat_g1v2
-- ============================================================

SELECT '>> Loading data into silver.erp_px_cat_g1v2...' AS info;
SELECT '   >> Truncating silver.erp_px_cat_g1v2...' AS info;
TRUNCATE TABLE silver.erp_px_cat_g1v2;
SELECT '   >> Truncate complete. Inserting rows...' AS info;
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT '>> silver.erp_px_cat_g1v2 loaded.' AS info;

SELECT '====================' AS info;
SELECT '>> Silver layer load complete.' AS info;
SELECT '====================' AS info;
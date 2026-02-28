-- ============================================================
-- Quality Checks: bronze.crm_cust_info
-- ============================================================
SELECT '>> [1/6] Starting quality checks for: bronze.crm_cust_info' AS progress;

-- Check 1: Duplicates / NULLs in primary key
SELECT '   [1/6 | Check 1] Duplicates / NULLs in primary key (cst_id)' AS progress;
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check 2: Unwanted spaces in string columns
SELECT '   [1/6 | Check 2] Unwanted spaces in string columns' AS progress;
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

SELECT cst_gndr FROM bronze.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr);

-- Check 3: Distinct values of coded columns
SELECT '   [1/6 | Check 3] Distinct values of coded columns (gender, marital status)' AS progress;
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

SELECT '>> [1/6] Completed quality checks for: bronze.crm_cust_info' AS progress;


-- ============================================================
-- Quality Checks: bronze.crm_prd_info
-- ============================================================
SELECT '>> [2/6] Starting quality checks for: bronze.crm_prd_info' AS progress;

-- Check 1: Duplicates / NULLs in primary key
SELECT '   [2/6 | Check 1] Duplicates / NULLs in primary key (prd_id)' AS progress;
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check 2: Split prd_key into cat_id and prd_key
SELECT '   [2/6 | Check 2] Deriving cat_id and prd_key from prd_key column' AS progress;
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7)                        AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- Check 3: Unwanted spaces in product name
SELECT '   [2/6 | Check 3] Unwanted spaces in product name (prd_nm)' AS progress;
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Check 4: NULLs or negative values in cost
SELECT '   [2/6 | Check 4] NULLs or negative values in cost (prd_cost)' AS progress;
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Check 5: Distinct values in product line
SELECT '   [2/6 | Check 5] Distinct values in product line (prd_line)' AS progress;
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

-- Check 6: Invalid date ranges (end before start)
SELECT '   [2/6 | Check 6] Invalid date ranges — end date before start date' AS progress;
SELECT prd_start_dt, prd_end_dt FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check 7: Derive correct end date using LEAD window function
SELECT '   [2/6 | Check 7] Deriving correct end date via LEAD() window function' AS progress;
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    (LEAD(prd_start_dt) OVER (
        PARTITION BY prd_key
        ORDER BY prd_start_dt
    ) - INTERVAL '1 day')::DATE AS prd_end_dt_derived
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

SELECT '>> [2/6] Completed quality checks for: bronze.crm_prd_info' AS progress;


-- ============================================================
-- Quality Checks: bronze.crm_sales_details
-- ============================================================
SELECT '>> [3/6] Starting quality checks for: bronze.crm_sales_details' AS progress;

-- Check 1: Unwanted spaces in order number
SELECT '   [3/6 | Check 1] Unwanted spaces in order number (sls_ord_num)' AS progress;
SELECT sls_ord_num FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- Check 2: Referential integrity — product key must exist in silver
SELECT '   [3/6 | Check 2] Referential integrity — sls_prd_key vs silver.crm_prd_info' AS progress;
SELECT sls_ord_num, sls_prd_key, sls_cust_id,
       sls_order_dt, sls_ship_dt, sls_due_dt,
       sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Check 3: Referential integrity — customer id must exist in silver
SELECT '   [3/6 | Check 3] Referential integrity — sls_cust_id vs silver.crm_cust_info' AS progress;
SELECT sls_ord_num, sls_prd_key, sls_cust_id,
       sls_order_dt, sls_ship_dt, sls_due_dt,
       sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check 4: Invalid date values
SELECT '   [3/6 | Check 4] Invalid / out-of-range date values (sls_order_dt)' AS progress;
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt::VARCHAR) <> 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;

-- Check 5: Invalid date order (order date after ship or due date)
SELECT '   [3/6 | Check 5] Date ordering — order date must not exceed ship/due date' AS progress;
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check 6: Data consistency — Sales = Quantity * Price
SELECT '   [3/6 | Check 6] Data consistency — sls_sales = sls_quantity * sls_price' AS progress;
SELECT DISTINCT
    sls_sales                                                          AS old_sales,
    sls_quantity,
    sls_price                                                          AS old_price,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0
          OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END                                                                AS sls_sales_derived,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END                                                                AS sls_price_derived
FROM bronze.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT '>> [3/6] Completed quality checks for: bronze.crm_sales_details' AS progress;


-- ============================================================
-- Quality Checks: bronze.erp_cust_az12
-- ============================================================
SELECT '>> [4/6] Starting quality checks for: bronze.erp_cust_az12' AS progress;

-- Check 1: Duplicates / NULLs in primary key
SELECT '   [4/6 | Check 1] Referential integrity — cid (stripped of NAS prefix) vs silver.crm_cust_info' AS progress;
SELECT 
    cid , 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4)
    ELSE cid
    END AS new_cid,
    bdate, 
    gen 
FROM bronze.erp_cust_az12
WHERE new_cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);


-- Check 2: Identify Out-of_range dates
SELECT '   [4/6 | Check 2] Out-of-range birth dates (bdate)' AS progress;
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;

-- Check 3: Data Standardization & Consistency
SELECT '   [4/6 | Check 3] Data standardization — distinct gender values (gen)' AS progress;
SELECT DISTINCT gen FROM bronze.erp_cust_az12;

SELECT '>> [4/6] Completed quality checks for: bronze.erp_cust_az12' AS progress;



-- ============================================================
-- Quality Checks: bronze.erp_loc_a101
-- ============================================================
SELECT '>> [5/6] Starting quality checks for: bronze.erp_loc_a101' AS progress;

-- Check 1: Duplicates / NULLs in primary key
SELECT '   [5/6 | Check 1] Referential integrity — cid (dashes removed) vs silver.crm_cust_info' AS progress;
SELECT REPLACE(cid,'-','') AS cid, 
cntry 
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','')  NOT IN (SELECT cst_key FROM silver.crm_cust_info);


--Check 2: country
SELECT '   [5/6 | Check 2] Data standardization — country name normalization (cntry)' AS progress;
SELECT DISTINCT 
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE cntry 
    END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

SELECT '>> [5/6] Completed quality checks for: bronze.erp_loc_a101' AS progress;


-- ============================================================
-- Quality Checks: bronze.erp_px_cat_g1v2
-- ============================================================
SELECT '>> [6/6] Starting quality checks for: bronze.erp_px_cat_g1v2' AS progress;

-- Check 1: Duplicates / NULLs in primary key
SELECT '   [6/6 | Check 1] Full table scan — review id, cat, subcat, maintenance' AS progress;
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


--Check 2: Unwanted spaces
SELECT '   [6/6 | Check 2] Unwanted spaces in cat and subcat columns' AS progress;
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat <> TRIM(subcat);


--Check 3: Data Standardization & Consistency
SELECT '   [6/6 | Check 3] Distinct category values (cat, subcat)' AS progress;
SELECT DISTINCT 
cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
subcat
FROM bronze.erp_px_cat_g1v2;

SELECT '>> [6/6] Completed quality checks for: bronze.erp_px_cat_g1v2' AS progress;
SELECT '>> All bronze layer quality checks complete.' AS progress;
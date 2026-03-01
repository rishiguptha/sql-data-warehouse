# Data Catalog — Gold Layer

> **Layer:** Gold | **Schema:** `gold` | **Pattern:** Star Schema  
> **Purpose:** Query-ready, business-facing tables optimized for analytical reporting and BI tools.  
> **Sources:** Cleaned and standardized data from the `silver` schema (CRM + ERP systems).

---

## Entity Overview

The gold layer implements a **star schema** with one fact table at the center and two dimension tables.

```
┌──────────────────┐         ┌──────────────────┐
│  dim_customers   │         │   dim_products   │
│  (10 columns)    │         │  (11 columns)    │
└────────┬─────────┘         └────────┬─────────┘
         │ customer_key (PK)           │ product_key (PK)
         │                             │
         └──────────┬──────────────────┘
                    │
            ┌───────▼────────┐
            │   fact_sales   │
            │  (9 columns)   │
            │                │
            │ customer_key(FK)│
            │ product_key (FK)│
            └────────────────┘
```

| Entity | Type | Row Grain | Primary Key |
|--------|------|-----------|-------------|
| `dim_customers` | Dimension | One row per unique customer | `customer_key` |
| `dim_products` | Dimension | One row per active product version | `product_key` |
| `fact_sales` | Fact | One row per sales order line | `order_number` + `product_key` |

---

## Entity Relationships

| Relationship | Type | Join Condition |
|---|---|---|
| `fact_sales` → `dim_customers` | Many-to-One | `fact_sales.customer_key = dim_customers.customer_key` |
| `fact_sales` → `dim_products` | Many-to-One | `fact_sales.product_key = dim_products.product_key` |

> **Note:** Surrogate keys (`customer_key`, `product_key`) are integer sequences generated with `ROW_NUMBER()`. They are not stable across full reloads — always join on surrogate keys, not on natural/business keys directly.

---

## Table: `gold.dim_customers`

**Description:** One record per unique customer, combining identity, demographic, and location data from both the CRM and ERP source systems. CRM is the master for gender; ERP is the fallback.

**Sources:** `silver.crm_cust_info` ✕ `silver.erp_cust_az12` ✕ `silver.erp_loc_a101`

| Column | Data Type | Key | Description | Example |
|--------|-----------|-----|-------------|---------|
| `customer_key` | INTEGER | PK | Surrogate key, generated via `ROW_NUMBER()` | `1`, `2`, `3` |
| `customer_id` | BIGINT | | Natural ID from CRM source system | `29449` |
| `customer_number` | VARCHAR | | Business-facing customer code | `AW00029449` |
| `first_name` | VARCHAR | | Customer first name (trimmed) | `Catherine` |
| `last_name` | VARCHAR | | Customer last name (trimmed) | `Abel` |
| `country` | VARCHAR | | Normalized country name from ERP | `United States`, `Germany` |
| `marital_status` | VARCHAR | | Standardized marital status | `Single`, `Married`, `n/a` |
| `gender` | VARCHAR | | CRM gender value; falls back to ERP if CRM = `n/a` | `Female`, `Male`, `n/a` |
| `birth_date` | DATE | | Date of birth from ERP; NULL if future date detected | `1971-08-15` |
| `create_date` | DATE | | Date the customer record was created in CRM | `2011-05-31` |

**Notes:**
- `marital_status` and `gender` are standardized to full words (e.g., `'M'` → `'Married'`); raw codes are replaced with `n/a` if unrecognized.
- Duplicate CRM records are deduplicated by keeping the most recent `cst_create_date`.

---

## Table: `gold.dim_products`

**Description:** One record per **active** product, enriched with category and subcategory data from the ERP product catalog. Historical product versions (where `prd_end_dt IS NOT NULL`) are excluded.

**Sources:** `silver.crm_prd_info` ✕ `silver.erp_px_cat_g1v2`

| Column | Data Type | Key | Description | Example |
|--------|-----------|-----|-------------|---------|
| `product_key` | INTEGER | PK | Surrogate key, generated via `ROW_NUMBER()` | `1`, `2`, `3` |
| `product_id` | BIGINT | | Natural ID from CRM source system | `210` |
| `product_number` | VARCHAR | | Business-facing product code (stripped key) | `HE-HL-U509-R` |
| `product_name` | VARCHAR | | Full product name | `Helmet 509 Racing` |
| `category_id` | VARCHAR | | Derived from the prefix of the raw `prd_key` | `AC_HE` |
| `category` | VARCHAR | | Product category from ERP catalog | `Accessories` |
| `subcategory` | VARCHAR | | Product subcategory from ERP catalog | `Helmets` |
| `maintenance` | BOOLEAN | | Whether the product requires maintenance | `true`, `false` |
| `product_cost` | BIGINT | | Unit cost; NULLs replaced with `0` | `35` |
| `product_line` | VARCHAR | | Standardized product line | `Mountain`, `Road`, `Sport`, `Touring`, `n/a` |
| `start_date` | DATE | | Date this product version became active | `2013-05-30` |

**Notes:**
- Only **current** product records are loaded (`WHERE prd_end_dt IS NULL`). Historical versions are excluded from the gold layer.
- `cat_id` is derived by taking the first 5 characters of the raw `prd_key` and replacing `-` with `_`.
- Product line codes are standardized: `'M'` → `'Mountain'`, `'R'` → `'Road'`, `'S'` → `'Sport'`, `'T'` → `'Touring'`.

---

## Table: `gold.fact_sales`

**Description:** One record per sales order line item. Stores transactional metrics (sales amount, quantity, price) with foreign keys into both dimension tables. Dates are cast from raw integer format (e.g., `20131201`) to `DATE`.

**Sources:** `silver.crm_sales_details` ✕ `gold.dim_products` ✕ `gold.dim_customers`

| Column | Data Type | Key | Description | Example |
|--------|-----------|-----|-------------|---------|
| `order_number` | VARCHAR | DD | Sales order identifier (degenerate dimension) | `SO43697` |
| `product_key` | INTEGER | FK | References `dim_products.product_key` | `214` |
| `customer_key` | INTEGER | FK | References `dim_customers.customer_key` | `1` |
| `order_date` | DATE | | Date the order was placed; NULL if invalid raw value | `2013-12-01` |
| `shipping_date` | DATE | | Date the order was shipped; NULL if invalid raw value | `2013-12-08` |
| `due_date` | DATE | | Date the order was due; NULL if invalid raw value | `2013-12-13` |
| `sales_amount` | BIGINT | | Corrected total sales (recalculated if inconsistent) | `2294` |
| `quantity` | BIGINT | | Number of units sold | `1` |
| `price` | BIGINT | | Unit price; derived from `sales / quantity` if invalid | `2294` |

**Notes:**
- **Data corrections applied in silver layer:**
  - Dates stored as integers (e.g., `20131201`) are cast to `DATE` using `STRPTIME`. Values of `0` or non-8-digit numbers become `NULL`.
  - `sales_amount` is recalculated as `quantity × |price|` if the raw value is NULL, zero, or inconsistent.
  - `price` is derived as `sales / quantity` if the raw price is NULL or ≤ 0.
- `order_number` is a **degenerate dimension** — it carries context but has no associated dimension table.

---

## Common Analytical Queries

```sql
-- Total sales by country
SELECT d.country, SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers d ON f.customer_key = d.customer_key
GROUP BY d.country
ORDER BY total_sales DESC;

-- Revenue by product category
SELECT p.category, SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Monthly sales trend
SELECT DATE_TRUNC('month', f.order_date) AS month,
       SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
GROUP BY month
ORDER BY month;
```

---

*Last updated: 2026-02-28 | Layer: Gold | Project: sql-data-warehouse*

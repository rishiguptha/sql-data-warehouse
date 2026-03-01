# SQL Data Warehouse — Portfolio Project

A modern data warehouse built with **DuckDB, Python, and Docker** using the Medallion Architecture (Bronze → Silver → Gold). Consolidates sales data from two source systems (CRM + ERP) into a star schema optimized for analytical reporting.

> Based on the [Data with Baara SQL Data Warehouse Project](https://www.youtube.com/playlist?list=PLNcg_FV9n7qaUWeyUkPfiVtMbKlrfMqA8), modified to use a cloud-native, serverless stack instead of SQL Server.

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| **DuckDB** | ≥ 1.4.4 | In-process analytical database (replaces SQL Server) |
| **Python** | ≥ 3.12 | Pipeline orchestration and ingestion scripts |
| **pandas** | ≥ 3.0.1 | Data manipulation (available for future transforms) |
| **Docker** | — | Containerized, reproducible environment *(in progress)* |
| **uv** | — | Fast Python package management |

---

## Architecture

![Architecture Diagram](docs/architecture.png)

| Layer | Role | Load Strategy |
|-------|------|---------------|
| **Bronze** | Raw ingestion — data as-is from source CSVs | `CREATE OR REPLACE TABLE` (full replace) |
| **Silver** | Cleaned & standardized — nulls, types, dedup, business rules | `TRUNCATE + INSERT` (full reload) |
| **Gold** | Star schema — query-ready materialized tables for BI | `DROP + CREATE` (full rebuild) |

### Data Flow

![Data Flow Diagram](docs/sql-datawarehouse-dataflow.png)

---

## Project Structure

```
sql-data-warehouse/
├── data/
│   └── warehouse.duckdb              # DuckDB database file (auto-created)
├── datasets/
│   ├── source_crm/                   # CRM source CSVs
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/                   # ERP source CSVs
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
├── docs/
│   ├── architecture.png              # Medallion architecture diagram
│   ├── Data Model.png                # Gold layer star schema (ER diagram)
│   ├── Integration-model.png         # CRM + ERP source integration model
│   ├── sql-datawarehouse-dataflow.png # End-to-end data flow diagram
│   └── data_catalog.md              # Gold layer data dictionary
├── scripts/
│   ├── bronze/
│   │   └── load_bronze.py           # Ingest CSVs → bronze schema
│   ├── silver/
│   │   └── load_silver.sql          # Clean + standardize → silver schema
│   ├── gold/
│   │   └── load_gold.sql            # Star schema → gold schema
│   ├── init_db.py                   # Create bronze/silver/gold schemas
│   └── run_pipeline.py              # Orchestrate full pipeline ⬜ in progress
├── tests/
│   ├── quality_checks_bronze.sql    # Row counts, nulls, duplicates in bronze
│   ├── quality_checks_silver.sql    # Referential integrity, standardization in silver
│   └── quality_checks_gold.sql      # FK integrity checks in gold
├── Dockerfile                        # ⬜ in progress
├── docker-compose.yml
├── pyproject.toml                    # Python project + dependencies (uv)
└── README.md
```

---

## Data Sources

Two simulated source systems, 6 CSV files total:

![Integration Model](docs/Integration-model.png)

| Schema | Table | Rows (approx.) | Description |
|--------|-------|----------------|-------------|
| CRM | `cust_info` | ~18K | Customer profiles — name, gender, marital status |
| CRM | `prd_info` | ~400 | Product catalog — name, cost, line, date ranges |
| CRM | `sales_details` | ~60K | Sales transactions — orders, quantities, prices |
| ERP | `CUST_AZ12` | ~18K | Customer demographics — birthdate, gender |
| ERP | `LOC_A101` | ~18K | Customer location — country |
| ERP | `PX_CAT_G1V2` | ~37 | Product categories and subcategories |

---

## Gold Layer — Star Schema

```
                    ┌──────────────────┐
                    │  dim_customers   │
                    │  customer_key PK │
                    └────────┬─────────┘
                             │ (FK)
┌──────────────────┐         │         ┌──────────────────┐
│   dim_products   │─────────┤         │                  │
│  product_key PK  │  (FK)   └────► fact_sales ◄──────────┘
└──────────────────┘
```

| Table | Columns | Grain | Description |
|-------|---------|-------|-------------|
| `gold.dim_customers` | 10 | One row per customer | CRM + ERP merged, gender master from CRM |
| `gold.dim_products` | 11 | One row per active product | CRM + ERP enriched, historical versions excluded |
| `gold.fact_sales` | 9 | One row per order line | Corrected sales metrics, surrogate FK lookups |

![Data Model](docs/data_model.png)

> 📖 Full column-level documentation: [`docs/data_catalog.md`](docs/data_catalog.md)

---

## Documentation

| File | Description |
|------|-------------|
| [`docs/architecture.png`](docs/architecture.png) | Medallion architecture overview (Bronze → Silver → Gold) |
| [`docs/sql-datawarehouse-dataflow.png`](docs/sql-datawarehouse-dataflow.png) | End-to-end data flow from source CSVs through all three layers |
| [`docs/Integration-model.png`](docs/Integration-model.png) | CRM + ERP source system integration model showing how tables are joined |
| [`docs/Data Model.png`](docs/Data%20Model.png) | Gold layer star schema ER diagram (dim_customers, dim_products, fact_sales) |
| [`docs/data_catalog.md`](docs/data_catalog.md) | Gold layer data dictionary — entity relationships, column definitions, business rules, and sample queries |

---


## Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| Bronze | `<source>_<entity>` | `crm_cust_info`, `erp_loc_a101` |
| Silver | `<source>_<entity>` (cleaned) | `crm_cust_info`, `erp_cust_az12` |
| Gold Dimensions | `dim_<entity>` | `dim_customers`, `dim_products` |
| Gold Facts | `fact_<entity>` | `fact_sales` |
| Surrogate keys | `<entity>_key` | `customer_key`, `product_key` |
| Audit columns | `dwh_<name>` | `dwh_load_date`, `dwh_create_date` |

---

## How to Run

### Prerequisites

```bash
pip install uv
uv sync
```

### Step 1 — Initialize the database

```bash
uv run scripts/init_db.py
```

Creates the `data/warehouse.duckdb` file with `bronze`, `silver`, and `gold` schemas.

### Step 2 — Load bronze layer

```bash
uv run scripts/bronze/load_bronze.py
```

Ingests all 6 CSV files into the `bronze` schema as-is, with a `dwh_load_date` audit column.

### Step 3 — Load silver layer

```bash
duckdb data/warehouse.duckdb < scripts/silver/load_silver.sql
```

Cleans, casts, deduplicates, and standardizes data into the `silver` schema.

### Step 4 — Load gold layer

```bash
duckdb data/warehouse.duckdb < scripts/gold/load_gold.sql
```

Builds `dim_customers`, `dim_products`, and `fact_sales` in the `gold` schema.

### Run quality checks

```bash
# After bronze load
duckdb data/warehouse.duckdb < tests/quality_checks_bronze.sql

# After silver load
duckdb data/warehouse.duckdb < tests/quality_checks_silver.sql

# After gold load
duckdb data/warehouse.duckdb < tests/quality_checks_gold.sql
```

---

## Pipeline Progress

| Layer | Status | Script |
|-------|--------|--------|
| Database Init | ✅ Complete | `scripts/init_db.py` |
| Bronze | ✅ Complete | `scripts/bronze/load_bronze.py` |
| Silver | ✅ Complete | `scripts/silver/load_silver.sql` |
| Gold | ✅ Complete | `scripts/gold/load_gold.sql` |
| Quality Checks | ✅ Complete | `tests/quality_checks_*.sql` |
| Data Catalog | ✅ Complete | `docs/data_catalog.md` |
| Orchestration | ⬜ Not Started | `scripts/run_pipeline.py` |
| Docker | ⬜ Not Started | `Dockerfile`, `docker-compose.yml` |

---

## Key Design Decisions

**DuckDB over SQL Server** — File-based, no server setup. Runs in-process with Python. Anyone can clone and run in 60 seconds with zero infrastructure.

**Materialized tables over views** — Gold layer writes data to disk. Downstream BI tools query pre-computed results, not live SQL re-executions. Production-grade pattern.

**Full load (Truncate & Insert)** — All 3 layers use full load. Silver uses `TRUNCATE + INSERT` for safe reloads without schema changes; Bronze uses `CREATE OR REPLACE`. Source data is small and historical — no need for incremental merge at this scale.

**Idempotent scripts** — Every script is safe to re-run. Bronze uses `CREATE OR REPLACE TABLE`, Silver uses `DROP + CREATE` for DDL then `TRUNCATE + INSERT` for data, Gold uses `DROP + CREATE`.

**CRM as master system for gender** — When CRM and ERP conflict on gender, CRM value is used. ERP is the fallback only when CRM returns `n/a`.

**Audit column on every table** — `dwh_load_date` (bronze) and `dwh_create_date` (silver) track when each record entered the warehouse. Required for debugging and lineage tracing.

**Silver cleans, Gold joins** — Data corrections (dedup, type casting, business rule standardization) happen in Silver. Gold is purely about joining and reshaping for analytical consumption.
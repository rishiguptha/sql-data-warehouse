# SQL Data Warehouse — Portfolio Project

A modern data warehouse built with **DuckDB, Python, and Docker** using the Medallion Architecture (Bronze → Silver → Gold). Consolidates sales data from two source systems (CRM + ERP) into a star schema optimized for analytical reporting.

> Based on the [Data with Baara SQL Data Warehouse Project](https://www.youtube.com/playlist?list=PLNcg_FV9n7qaUWeyUkPfiVtMbKlrfMqA8), modified to use a cloud-native, server-less stack instead of SQL Server.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **DuckDB** | In-process analytical database (replaces SQL Server) |
| **Python** | Pipeline orchestration and ingestion scripts |
| **Docker** | Containerized, reproducible environment |
| **uv** | Fast Python package management |

---

## Architecture

```
Source CSVs (CRM + ERP)
        │
        ▼
┌─────────────────┐
│  Bronze Layer   │  Raw ingestion — as-is from source, full load
│  (Ingest)       │  + dwh_load_date audit column
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  Clean & standardize — nulls, types, dedup
│  (Clean)        │  Data normalization + derived columns
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  Star schema — dim_customers, dim_products,
│  (Business)     │  fact_sales — materialized tables, query-ready
└─────────────────┘
```

Architecture diagram: [`docs/architecture.png`](docs/architecture.png)

---

## Project Structure

```
sql-data-warehouse/
├── data/
│   └── warehouse.duckdb          # DuckDB database file
├── datasets/
│   ├── source_crm/               # CRM source CSVs
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/               # ERP source CSVs
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
├── docs/
│   └── architecture.png
├── scripts/
│   ├── bronze/
│   │   └── load_bronze.py        # Ingest CSVs → bronze schema
│   ├── silver/
│   │   └── load_silver.sql       # Clean + standardize → silver schema
│   ├── gold/
│   │   └── load_gold.sql         # Star schema → gold schema
│   ├── init_db.py                # Create schemas
│   └── run_pipeline.py           # Orchestrate full pipeline
├── tests/
│   └── quality_checks_bronze.sql # Row counts, nulls, duplicates
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Data Sources

Two simulated source systems, 6 CSV files total:

| Schema | Table | Description |
|--------|-------|-------------|
| CRM | `cust_info` | Customer profiles — name, gender, marital status |
| CRM | `prd_info` | Product catalog — name, cost, line, dates |
| CRM | `sales_details` | Sales transactions — orders, quantities, prices |
| ERP | `CUST_AZ12` | Customer demographics — birthdate, gender |
| ERP | `LOC_A101` | Customer location — country |
| ERP | `PX_CAT_G1V2` | Product categories and subcategories |

---

## Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| Bronze | `<source>_<entity>` | `crm_cust_info`, `erp_loc_a101` |
| Silver | `<source>_<entity>` | `crm_cust_info` (cleaned) |
| Gold Dimensions | `dim_<entity>` | `dim_customers`, `dim_products` |
| Gold Facts | `fact_<entity>` | `fact_sales` |
| Surrogate keys | `<entity>_key` | `customer_key`, `product_key` |
| Audit columns | `dwh_<name>` | `dwh_load_date`, `dwh_source` |

---

## How to Run

### Option 1: Python (local)

```bash
# Install dependencies
pip install uv
uv sync

# Initialize database
uv run scripts/init_db.py

# Run full pipeline
uv run scripts/run_pipeline.py
```

### Option 2: Docker

```bash
docker-compose up
```

---

## Pipeline Progress

| Layer | Status | Script |
|-------|--------|--------|
| Bronze | ✅ Complete | `scripts/bronze/load_bronze.py` |
| Silver | 🔄 In Progress | `scripts/silver/load_silver.sql` |
| Gold | ⬜ Not Started | `scripts/gold/load_gold.sql` |
| Orchestration | ⬜ Not Started | `scripts/run_pipeline.py` |
| Docker | ⬜ Not Started | `Dockerfile`, `docker-compose.yml` |

---

## Key Design Decisions

**DuckDB over SQL Server** — File-based, no server setup. Runs in-process with Python. Recruiter can clone and run in 60 seconds with zero infrastructure.

**Materialized tables over views** — Gold layer writes data to disk. Downstream BI tools query pre-computed results, not live SQL re-executions. Production-grade pattern.

**Full load (Truncate & Insert)** — All 3 layers use full load. Source data is small and historical; no need for incremental merge at this scale.

**Audit column on every table** — `dwh_load_date` tracks when each record entered the warehouse. Required for debugging and lineage tracing.
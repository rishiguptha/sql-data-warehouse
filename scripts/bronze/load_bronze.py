# ============================================================
# Script  : load_bronze.py
# Layer   : Bronze
# Purpose : Load raw CSV files from source systems (CRM and ERP)
#           into the bronze schema as-is, with minimal transformation.
#           Adds a dwh_load_date timestamp column to track ingestion time.
#           This script is idempotent — each table is fully replaced
#           using CREATE OR REPLACE TABLE on every run.
# Source  : datasets/source_crm/, datasets/source_erp/
# Target  : bronze.crm_cust_info, bronze.crm_prd_info,
#           bronze.crm_sales_details, bronze.erp_cust_az12,
#           bronze.erp_loc_a101, bronze.erp_px_cat_g1v2
# ============================================================

import duckdb
import time


DB_PATH = "data/warehouse.duckdb"


def load_bronze():
    """
    Load data from CSV files into the bronze schema.
    Args:
        None

    Returns:
        None
    """
    bronze_tables = [
        ("crm_cust_info",    "datasets/source_crm/cust_info.csv"),
        ("crm_prd_info",     "datasets/source_crm/prd_info.csv"),
        ("crm_sales_details","datasets/source_crm/sales_details.csv"),
        ("erp_cust_az12",    "datasets/source_erp/CUST_AZ12.csv"),
        ("erp_loc_a101",     "datasets/source_erp/LOC_A101.csv"),
        ("erp_px_cat_g1v2",  "datasets/source_erp/PX_CAT_G1V2.csv")
    ]

    print("====================")
    print(">> Loading CSV files into bronze schema...")

    total_start = time.time()

    try:
        con = duckdb.connect(DB_PATH)
        for table_name, file_path in bronze_tables:
            table_start = time.time()
            print(f">> Loading {table_name}...")
            con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('{file_path}', AUTO_DETECT = TRUE)")
            table_duration = time.time() - table_start
            print(f"   >> {table_name} loaded. ({table_duration:.2f}s)")
        print(">> Data loaded into bronze schema.")
    except Exception as e:
        print(f">> ERROR: {e}")
        raise
    finally:
        con.close()

    total_duration = time.time() - total_start
    print(f">> Total bronze load duration: {total_duration:.2f}s")
    print("====================")


if __name__ == "__main__":
    load_bronze()
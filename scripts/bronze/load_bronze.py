import duckdb


DB_PATH = "data/warehouse.duckdb"

def load_bronze():

    """
    Load data from CSV files into the bronze schema.
    Args:
        None

    Returns:
        None
    """

    print(">> Loading data into bronze schema...")

    con = duckdb.connect(DB_PATH)
    print(">> Loading cust_info table.")
    con.execute("CREATE OR REPLACE TABLE bronze.crm_cust_info AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('datasets/source_crm/cust_info.csv', AUTO_DETECT = TRUE)")

    print(">> Loading prd_info table.")
    con.execute("CREATE OR REPLACE TABLE bronze.crm_prd_info AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('datasets/source_crm/prd_info.csv', AUTO_DETECT = TRUE)")

    print(">> Loading sales_details table.")
    con.execute("CREATE OR REPLACE TABLE bronze.crm_sales_details AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('datasets/source_crm/sales_details.csv', AUTO_DETECT = TRUE)")

    print(">> Loading cust_az12 table.")
    con.execute("CREATE OR REPLACE TABLE bronze.erp_cust_az12 AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('datasets/source_erp/CUST_AZ12.csv', AUTO_DETECT = TRUE)")

    print(">> Loading loc_a101 table.")
    con.execute("CREATE OR REPLACE TABLE bronze.erp_loc_a101 AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('datasets/source_erp/LOC_A101.csv', AUTO_DETECT = TRUE)")

    print(">> Loading px_cat_g1v2 table.")
    con.execute("CREATE OR REPLACE TABLE bronze.erp_px_cat_g1v2 AS SELECT *, CURRENT_TIMESTAMP AS dwh_load_date FROM read_csv('datasets/source_erp/PX_CAT_G1V2.csv', AUTO_DETECT = TRUE)")

    print(">> Data loaded into bronze schema.")
    con.close()

    


if __name__ == "__main__":
    load_bronze()
    
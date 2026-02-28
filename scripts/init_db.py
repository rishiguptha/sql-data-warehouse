# ============================================================
# Script  : init_db.py
# Layer   : Infrastructure
# Purpose : Initialize the DuckDB warehouse database by creating
#           the three-layer medallion architecture schemas:
#           bronze (raw), silver (cleaned), and gold (aggregated).
#           This script must be run once before any data loading.
#           It is safe to re-run — schemas are created only if
#           they do not already exist (CREATE SCHEMA IF NOT EXISTS).
# Target  : data/warehouse.duckdb
#           Schemas: bronze, silver, gold
# ============================================================

import duckdb

DB_PATH = "data/warehouse.duckdb"

def init_database():

    """
    Initialize the database by creating the necessary schemas.
    Args:
        None

    Returns:
        None
    """
    
    print(f">> Connecting to database at {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    
    print(">> Creating schemas...")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    
    print(">> Database initialized successfully.")
    print(">> Schemas: bronze, silver, gold")
    
    con.close()

if __name__ == "__main__":
    init_database()
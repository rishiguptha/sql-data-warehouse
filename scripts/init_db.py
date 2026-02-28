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
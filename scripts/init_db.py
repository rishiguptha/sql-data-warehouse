import duckdb

DB_PATH = "warehouse.duckdb"

def init_database():
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
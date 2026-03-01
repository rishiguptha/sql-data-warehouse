import duckdb
import time
from bronze.load_bronze import load_bronze
from init_db import init_database

DB_PATH = "data/warehouse.duckdb"

def load_silver(connection):
    print(">> [2/3] Loading silver layer...")
    layer_start = time.time()

    try:
        with open("scripts/silver/load_silver.sql", "r") as f:
            connection.executescript(f.read())

    except Exception as e:
        print(f">> Error: {e}")
        raise
    print(f">> [2/3] Silver layer complete. ({time.time() - layer_start:.2f}s)")



def load_gold(connection):
    print(">> [3/3] Loading gold layer...")
    layer_start = time.time()

    try:
        with open("scripts/gold/load_gold.sql", "r") as f:
            connection.executescript(f.read())

    except Exception as e:
        print(f">> Error: {e}")
        raise
    print(f">> [3/3] Gold layer complete. ({time.time() - layer_start:.2f}s)")


def run_pipeline():
    print("====================")
    print(">> Starting pipeline...")
    print("====================")

    start_time = time.time()
    try:
        init_database()
    except Exception as e:
        print(f">> ERROR: {e}")
        raise
    
    try:
        load_bronze()
    except Exception as e:
        print(f">> ERROR: {e}")
        raise
    
    try:
        connection = duckdb.connect(DB_PATH)

        load_silver(connection)

        load_gold(connection)
    except Exception as e:
        print(f">> ERROR: {e}")
        raise
    finally:
        connection.close()

    total_duration = time.time() - start_time

    print("====================")
    print(f">> Pipeline End.  Total: ({total_duration:.2f}s)")
    print("====================")

def main():
    run_pipeline()


if __name__ == "__main__":
    main()

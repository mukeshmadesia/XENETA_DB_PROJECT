import duckdb
import os



def get_duckdb_connection():
    # Determine the base directory (the root of your project)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Construct the path to the DuckDB database file
    db_path = os.path.join(base_dir, 'dev.duckdb')

    # Connect to the DuckDB database file
    conn = duckdb.connect(db_path)
    
    return conn



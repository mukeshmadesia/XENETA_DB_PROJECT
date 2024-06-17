from xeneta_dbutil import get_duckdb_connection
import os
import sys


print("Initial Setup started..")

print("!!!WARNING!!! If you are connected to local duckdb, please disconnect to proceed - Use Command- CTRL + D")


print("!!!ALERT!!! The all raw tables records will be deleted if exists and set in initial state")

confirmation = input("Are you sure you want to delete these tables? (yes/no): ")
if confirmation.lower() == 'yes':
    print("Tables deletion Starting...")
else:
    print("Operation cancelled.")
    sys.exit(0)

# Get the DuckDB connection
conn = get_duckdb_connection()

conn.execute(
 """ CREATE SCHEMA IF NOT EXISTS raw;"""
 )

conn.execute(
 """ CREATE SCHEMA IF NOT EXISTS staging;"""
 )

conn.execute(
 """ CREATE SCHEMA IF NOT EXISTS final;"""
 )

# Create the table (only needs to be run once)
conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.file_metadata (
        file_id INTEGER NOT NULL,
        file_name VARCHAR NOT NULL,
        file_path VARCHAR NOT NULL,
        processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR,
        remarks VARCHAR,
        PRIMARY KEY (file_id, file_name)
    );
""")


conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.charges (
            D_ID INTEGER,
            CURRENCY VARCHAR,
            CHARGE_VALUE DOUBLE
        );
    """)


conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.datapoints (
        D_ID INTEGER,
        CREATED TIMESTAMP,
        ORIGIN_PID INTEGER,
        DESTINATION_PID INTEGER,
        VALID_FROM DATE,
        VALID_TO DATE,
        COMPANY_ID INTEGER,
        SUPPLIER_ID INTEGER,
        EQUIPMENT_ID INTEGER
    );
""")

# Clear the table in Intial state
conn.execute(f"DELETE FROM raw.file_metadata;")

# Clear the table in Intial state
conn.execute(f"DELETE FROM raw.charges;")

# Clear the table in Intial state
conn.execute(f"DELETE FROM raw.datapoints;")

# Clear the table in Intial state
conn.execute(f"DELETE FROM raw.ports;")

# Clear the table in Intial state
conn.execute(f"DELETE FROM raw.regions;")

# Clear the table in Intial state
conn.execute(f"DELETE FROM raw.exchange_rates;")

# Close the connection
conn.close()

print("Initial Setup completed..")
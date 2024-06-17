from xeneta_dbutil import get_duckdb_connection
import os


# Function to load constant CSV files into Table
def csv_data_loader(tablename, csv_file):

    # Get the DuckDB connection
    conn = get_duckdb_connection()

    # Determine the base directory (the root of your project)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Construct the path to the CSV file
    csv_dir = os.path.join(base_dir, 'input_files')
 
    csv_path = os.path.join(csv_dir, csv_file)

    # Parameterize the CSV path in the SQL query
    sql_query = f"""
        CREATE OR REPLACE TABLE raw.{tablename} AS
        SELECT * FROM read_csv_auto('{csv_path}')
    """

    # Execute the SQL query
    conn.execute(sql_query)
    print(f"Table '{tablename}' successfully created and loaded")

    # Close the connection
    conn.close()


def main():
    csv_data_loader('regions','DE_casestudy_regions.csv')
    csv_data_loader('ports','DE_casestudy_ports.csv')
    csv_data_loader('exchange_rates','DE_casestudy_exchange_rates.csv')

 


if __name__ == "__main__":
    print("Inside Static Data Loader")
    main()
    


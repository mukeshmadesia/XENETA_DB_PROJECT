from xeneta_dbutil import get_duckdb_connection
import os


# Function to load data CSV file into Table
def csv_batch_loader(tablename):

    # Get the DuckDB connection
    conn = get_duckdb_connection()

    # Determine the base directory (the root of your project)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  

    # Fetch the last processed file_id from file_metadata table
    result = conn.execute(f"""SELECT max(file_id) FROM raw.file_metadata WHERE file_name = '{tablename}'""").fetchone()

    last_file_id_processed = result[0] if result[0] is not None else 0
    file_id_to_upload = last_file_id_processed + 1
    filename_to_upload = f"DE_casestudy_{tablename}_{file_id_to_upload}.csv"

    # Construct the path to the CSV file
    csv_dir = os.path.join(base_dir, 'input_files')

    csv_path = os.path.join(csv_dir, filename_to_upload)


    # Validate if file is present or not
    if os.path.exists(csv_path):

        print(f"file_name - {filename_to_upload} : Loading Started...")
        # Parameterize the CSV path in the SQL query
        sql_query = f"""
           COPY raw.{tablename} FROM '{csv_path}' (AUTO_DETECT FALSE, HEADER TRUE);
        """

        try:
            # Execute the SQL query
            conn.execute(sql_query)
            print(f"Table '{tablename}' successfully loaded with filename: {filename_to_upload}")
        except Exception as e:
            print(f"Error loading data into {tablename}: {e}")
        

        #Insert the file_id processed in file_metadata Table
        conn.execute(" INSERT INTO raw.file_metadata (file_id, file_name, file_path, status, remarks) VALUES (?, ?, ?, ?, ?)",
              [file_id_to_upload, tablename, 'input_files', 'processed', 'Processed successfully'])

    else:
        #TODO Add logger
        print(f"file_id = {file_id_to_upload}, file_name = {filename_to_upload} has not been received yet")
    
    # Close the connection
    conn.close()

def main():
    csv_batch_loader('charges')
    csv_batch_loader('datapoints')


if __name__ == "__main__":
    print("Inside Main Batch Loader")
    main()


 # Update metadata table


# file_name = 'charges'
# result = conn.execute(f"""SELECT max(file_id) FROM raw.file_metadata WHERE file_name = '{file_name}'""").fetchone()
# file_id_processed = result[0] if result[0] is not None else 0

# print (file_id_processed)

# file_id = file_id_processed + 1
# conn.execute(" INSERT INTO raw.file_metadata (file_id, file_name, file_path, status, remarks) VALUES (?, ?, ?, ?, ?)",
#               [file_id, file_name, 'input_files', 'processed', 'Processed successfully'])



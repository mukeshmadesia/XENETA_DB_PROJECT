# Project Details

## Project Local Set Up 
1. Clone Repo in local
2. Install Poetry 
3. Install dependencies - python, duckdb, dbt-duckdb
4. Validate ~/.dbt/profile.yml contains duckdb config 

## Project execution 
1. For Initial Setup - Schema, Metadata table creation 
   
    ```console
    python database_load_script/initial_db_setup.py
    ``` 
2. For Static data load - Port, Region and Exchange Rate data load
    ``` 
    python database_load_script/static_data_load.py 
    ```
3. For batch load, This can be executed multiple times or scheduled to run daily after receiving new datapoints and charges file.
    ```
    python database_load_script/batch_load.py
    ```
4. `dbt run` to Run Models from `cd lane_price_estimation`
    
    Project accepts `start_date`, `end_date`, `company_coverage`, `supplier_coverage`
    default is configured in `dbt_project.yml`

    ````
    dbt run --vars '{"start_date": "2021-03-01", "end_date": "2021-03-01", "company_coverage": 5, "supplier_coverage": 2}'
    ```
5. `dbt test` to run Test added in dbt project



## Project Structure

            xeneta_db_project/
            │
            ├── dev.duckdb
            ├── lane_price_estimation/          #   dbt project
            ├── database_load_script/           
            │   └── initial_db_setup.py         # Initial setup of database
            │   └── static_data_load.py         # Load Static data
            │   └── batch_load.py               # To load batch data
            │   └── xeneta_dbutils.py           # common function
            ├── input_files/
            ├── pyproject.toml
            └── other_files_and_folders/



## Transformation
### Table

#### charges_aggregated
* Introduced lane_id by Concatening - Origin_port_id(4 digit) + 0 + Destination_port_id(4 digit) 
* 


#### ports
* PID is not unique is raw csv provided - Used Row_Number() function and removed duplicate occurance
* Severity is muted to  `warning` for known error `2` duplicate records

#### ports_error_stage
* Table contains Duplicate port details. Skipped from processing

### 

## Macros
* Added a Macro - to overide the schema naming convention by DBT - otherwise DBT add default_schema('main') as prefix 
* Added Macro to Create counter table used as on-run-start: hook 

## Models

### raw
* To validate raw data (e.g. 'Not Null' and Primary Key are Unique)
* Used materialization as Ephemeral - No additional storage used

### stage
* To Transform data and remove error data if found any
* Used materialization as View for static data - For Error data used Table - For Batch incremental data used Table

### final
* Aggregated and final count data



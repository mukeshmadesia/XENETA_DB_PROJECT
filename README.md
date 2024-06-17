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


## Functional Details
* Data (datapoints and charges) will be loaded in batch or scheduled manner
* file_metadata - Table introduced to keep track of file processed and to be used for restart in case of batch failure of multiple file.
* Each run will load one pair (datapoints & charges). Currently loads one pair per run, can be rerun or modified to handle more in single execution as required.
* Each Run will Automatically identify the next filename to be picked for load.
* Estimation is done for each day within provided Date Range(start and end date). Provide same date as start and end for single day.
* Data pipeline - 
    1. Create a series of dates(estimation_date) within Date Range.
    2. Identify valid datapoints for each estimation_date i.e. datapoints should be valid on estimation date.
    3. Sum up the charges for each estimation date as per corresponding exchange rate
    4. Calculated Average and Median charges for each estimation date for given lane_id, equipment_id.
    5. Created a counter metadata to be populated before and after each run.

## Pipeline

### Data Load (database_load_script/)

#### Intial DB Setup(initial_db_setup.py)
* Run to create - Schema and file_metadata
* Will give Warning and Alert - As it will initialize the database in initial state
* type `yes`if you want to continue initialization.

#### static_data_load.py
* Will load - Ports, Regions and Exchange Rate date

#### Batch Load (batch_load.py)
* This can be re-run as and when new file is received.
* It refers file_metadata table to ensure no duplicate file loaded and to identify next file_id to be processed.
* If next file is not present in location, It will message about that and process ends gracefully.
* As of now serial number is used as ID, can be modified as required.


### Estimation, Transformation (lane_price_estimation/)

### NEW Fields (Concepts)

#### Lane ID (lane_id)
* New data introduced lane_id by Concatening - Origin_port_id(4 digit) + 0 + Destination_port_id(4 digit) - to uniquely identify Route

#### Estimation Date (estimation_date)
* Estimantion can done for given date or range of date 
* Estimation date can be dynamically provided and Default can be provided in dbt_project.yml
* For Range - start_date and end_date both should be different
* For Single day - start_date and end_date should be kept same

#### Company Coverage Limit (company_coverage_ok)
* Minimum number of company - to consider the lane estimation ok
* Default value in `dbt_project.yml`as 5
* Can be dynamically provided - in `vars`while `dbt run`

#### Supplier Coverage Limit (supplier_coverage_ok)
* Minimum number of Supplier - to consider the lane estimation ok
* Default value in `dbt_project.yml`as 2
* Can be dynamically provided - in `vars`while `dbt run`

#### Data Quality (dq_ok)
* Lane_id and Equipment ID is assumed OK for estimation when data provided by atleast `company_coverage_ok` company and `supplier_coverage_ok` supplier

### Table 

#### charges_aggregated (Schema=final)
* Introduced lane_id by Concatening - Origin_port_id(4 digit) + 0 + Destination_port_id(4 digit) 
* Average and Median charge for Given estmation date, lane_id, equipment_id along with dq_ok flag

#### counter (Schema=final)
* Before and After Counts of lane_id, equipemnt_id, origin_port_id, destination_port_id having data quality Ok.

#### ports
* PID is not unique is raw csv provided - Used Row_Number() function and removed duplicate occurance
* Severity is muted to  `warning` for known error `2` duplicate records

#### ports_error_stage
* Table contains Duplicate port details. Skipped from processing

#### file_metadata
* This stores details of file processed successfully 
* Pipeline will refer this to check which last file is processed and will start with next file-id.

***N.B. : Refer dbt doc for other table details***


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



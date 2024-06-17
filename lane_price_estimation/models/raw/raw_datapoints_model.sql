-- Define a Common Table Expression (CTE) to select data from raw source
WITH cte_raw_datapoints AS (
    SELECT
        D_ID AS datapoint_id,
        CREATED AS created_at,
        ORIGIN_PID AS origin_port_id,
        DESTINATION_PID AS destination_port_id,
        VALID_FROM AS valid_from_date,
        VALID_TO AS valid_to_date,
        COMPANY_ID AS company_id,
        SUPPLIER_ID AS supplier_id,
        EQUIPMENT_ID AS equipment_id
    FROM {{ source('raw', 'datapoints') }}
)

-- Select columns from the CTE 
SELECT
    datapoint_id,
    created_at,
    origin_port_id,
    destination_port_id,
    valid_from_date,
    valid_to_date,
    company_id,
    supplier_id,
    equipment_id
FROM cte_raw_datapoints

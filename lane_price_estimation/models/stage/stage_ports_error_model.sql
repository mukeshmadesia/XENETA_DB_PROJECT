{{ config(materialized='view', schema='stage', alias='ports_error_stage') }}

WITH cte_duplicated_ports AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY port_id ORDER BY port_id) AS row_num
    FROM {{ ref('raw_ports_model') }}
)


-- Select columns from the CTE which failed the validations 
SELECT
    port_id,
    port_code,
    region_slug,
    port_name,
    port_country,
    country_code,
    'Duplicate Port' AS error_reason
FROM cte_duplicated_ports
WHERE row_num > 1

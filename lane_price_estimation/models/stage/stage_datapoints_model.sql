
{{ config(materialized='incremental', schema='stage', alias='datapoints_stage') }}

WITH cte_stage_datapoints AS (
    SELECT *
    FROM {{ ref('raw_datapoints_model') }}
)


-- Select columns from the CTE with necessary validations
SELECT
    datapoint_id,
    created_at,
    origin_port_id,
    destination_port_id,
    valid_from_date,
    valid_to_date,
    company_id,
    supplier_id,
    equipment_id,
    LPAD(CAST(origin_port_id AS VARCHAR), 4, '0') || '0' || LPAD(CAST(destination_port_id AS VARCHAR), 4, '0') AS lane_id
FROM cte_stage_datapoints
WHERE
    datapoint_id IS NOT NULL
    AND created_at IS NOT NULL
    AND origin_port_id IS NOT NULL
    AND destination_port_id IS NOT NULL
    AND valid_from_date IS NOT NULL
    AND valid_to_date IS NOT NULL
    AND company_id IS NOT NULL
    AND supplier_id IS NOT NULL
    AND equipment_id IS NOT NULL
{% if is_incremental() %}
    AND datapoint_id not in (select datapoint_id from {{ this }})
{% endif %}
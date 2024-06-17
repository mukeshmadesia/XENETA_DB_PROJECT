{{ config(materialized='table', schema='stage', alias='charges_stage') }}

WITH cte_stage_charges AS (
    SELECT *
    FROM {{ ref('raw_charges_model') }}
)


-- Select columns from the CTE with necessary validations

SELECT
    datapoint_id,
    currency,
    charge_value
FROM cte_stage_charges
WHERE datapoint_id IS NOT NULL
  AND currency IS NOT NULL
  AND charge_value IS NOT NULL
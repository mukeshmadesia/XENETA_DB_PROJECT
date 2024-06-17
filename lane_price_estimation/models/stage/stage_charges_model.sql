{{ config(materialized='incremental', schema='stage', alias='charges_stage') }}

WITH cte_stage_charges AS (
    SELECT 
        sd.datapoint_id,
        sd.currency,
        sd.charge_value
    FROM {{ ref('raw_charges_model') }} sd
{% if is_incremental() %}
    left join {{ this }} t
    on sd.datapoint_id = t.datapoint_id
    and sd.currency = t.currency
    where t.datapoint_id is null  
{% endif %}

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
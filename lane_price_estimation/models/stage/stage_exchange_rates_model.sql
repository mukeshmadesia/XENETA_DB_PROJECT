
{{ config(materialized='view', schema='stage', alias='exchange_rates_stage') }}

WITH cte_stage_exchange_rates AS (
    SELECT *
    FROM {{ ref('raw_exchange_rates_model') }}
)


-- Select columns from the CTE with necessary validations
SELECT
    exchange_rate_date,
    currency,
    rate
FROM cte_stage_exchange_rates
WHERE
    exchange_rate_date IS NOT NULL
    AND currency IS NOT NULL
    AND rate IS NOT NULL
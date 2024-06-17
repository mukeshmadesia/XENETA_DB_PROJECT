-- Define a Common Table Expression (CTE) to select data from raw source
WITH cte_raw_exchange_rates AS (
    SELECT
        DAY AS exchange_rate_date,
        CURRENCY AS currency,
        RATE AS rate
    FROM {{ source('raw', 'exchange_rates') }}
)

-- Select all columns from the CTE 
SELECT
    exchange_rate_date,
    currency,
    rate
FROM cte_raw_exchange_rates


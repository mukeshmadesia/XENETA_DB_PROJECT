WITH cte_raw_charges AS (
    SELECT
        D_ID AS datapoint_id,
        CURRENCY AS currency,
        CHARGE_VALUE AS charge_value
    FROM {{ source('raw', 'charges') }}
)

SELECT
    datapoint_id,
    currency,
    charge_value
FROM cte_raw_charges
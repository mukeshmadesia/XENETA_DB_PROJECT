

{{ config(materialized='view', schema='final', alias='dp_total_charge') }}

-- Create a Table of sequence dates within given dynamic range required for estimation Reporting

WITH date_sequence AS (
    SELECT
        generate_series(
            '{{ var("start_date") }}'::DATE, 
            '{{ var("end_date") }}'::DATE, 
            INTERVAL 1 DAY)
            AS estimation_dates
),


date_range AS (
    SELECT CAST(UNNEST(estimation_dates) AS DATE) AS estimation_date FROM date_sequence
),


-- Cross Join Datapoints with Each date in date_range and get only those are valid on that day
datapoint_dates AS (
    SELECT
        d.estimation_date,
        dp.datapoint_id,
        dp.company_id,
        dp.supplier_id,
        dp.equipment_id,
        dp.lane_id
    FROM date_range d
    CROSS JOIN {{ ref('stage_datapoints_model') }} dp
    WHERE d.estimation_date BETWEEN dp.valid_from_date AND dp.valid_to_date
),


charges_detail AS (
    SELECT
        c.datapoint_id,
        c.currency,
        c.charge_value
    FROM {{ ref('stage_charges_model') }} c
),

exchange_rates AS (
    SELECT 
        e.exchange_rate_date,
        e.currency,
        e.rate
    FROM {{ ref('stage_exchange_rates_model') }} e
),


charges_with_exchange_rates AS (
    SELECT
        ds.estimation_date,
        ds.lane_id,
        ds.company_id,
        ds.supplier_id,
        ds.equipment_id,
        ds.datapoint_id,
        cd.charge_value,
        (cd.charge_value / er.rate) AS charge_value_usd
    FROM datapoint_dates ds
    JOIN charges_detail cd ON ds.datapoint_id = cd.datapoint_id
    JOIN exchange_rates er ON ds.estimation_date = er.exchange_rate_date AND cd.currency = er.currency
),

dp_total_charges_by_day AS (
    SELECT
        estimation_date,
        datapoint_id,
        SUM(charge_value_usd) AS total_charge_usd
    FROM charges_with_exchange_rates
    GROUP BY estimation_date, datapoint_id
),


dp_total_charge_detail AS (
    SELECT
        tc.estimation_date,
        dp.datapoint_id,
        dp.lane_id,
        dp.company_id,
        dp.supplier_id,
        dp.equipment_id,
        tc.total_charge_usd
    FROM dp_total_charges_by_day tc
    JOIN {{ ref('stage_datapoints_model') }} dp ON tc.datapoint_id = dp.datapoint_id

)

SELECT
    estimation_date,
    datapoint_id,
    lane_id,
    company_id,
    supplier_id,
    equipment_id,
    total_charge_usd
FROM dp_total_charge_detail 



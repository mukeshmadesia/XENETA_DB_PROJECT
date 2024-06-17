
{{ config(materialized='view', schema='final', alias='lane_coverage') }}

With lane_supplier_company_coverage AS (

    SELECT
        estimation_date,
        lane_id,
        equipment_id,
        count(distinct company_id) as company_coverage,
        count(distinct supplier_id) as supplier_coverage,
    FROM  {{ ref('datapoint_total_charge_by_day')}} 
    Group by estimation_date, lane_id, equipment_id

)

Select * from lane_supplier_company_coverage


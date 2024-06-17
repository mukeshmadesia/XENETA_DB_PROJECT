{{ config(materialized='view', schema='stage', alias='ports_stage') }}

WITH cte_stage_ports AS (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY port_id ORDER BY port_id) AS row_num
FROM {{ ref('raw_ports_model') }}
WHERE port_id IS NOT NULL
    AND port_code IS NOT NULL
    AND region_slug IS NOT NULL
    AND port_name IS NOT NULL
    AND port_country IS NOT NULL
    AND country_code IS NOT NULL
)


SELECT
    port_id,
    port_code,
    region_slug,
    port_name,
    port_country,
    country_code
FROM cte_stage_ports
WHERE row_num = 1

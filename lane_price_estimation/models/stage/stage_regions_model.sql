
{{ config(materialized='view', schema='stage', alias='regions_stage') }}

WITH cte_stage_regions AS (
    SELECT *
    FROM {{ ref('raw_regions_model') }}
)


-- Select columns from the CTE with necessary validations
SELECT
    region_slug,
    region_name,
    region_parent_slug
FROM cte_stage_regions
WHERE
    region_slug IS NOT NULL
    AND region_name IS NOT NULL
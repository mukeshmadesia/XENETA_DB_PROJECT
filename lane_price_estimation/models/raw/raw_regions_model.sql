WITH cte_raw_regions AS (
    SELECT
        SLUG AS region_slug,
        NAME AS region_name,
        PARENT AS region_parent_slug 
    FROM {{ source('raw', 'regions') }}
)



SELECT
    region_slug,
    region_name,
    region_parent_slug
FROM cte_raw_regions



WITH cte_raw_ports AS (
 SELECT
 *
 FROM {{source('raw','ports')}}

)

SELECT
    PID AS port_id,
    CODE AS port_code,
    SLUG AS region_slug,
    NAME AS port_name,
    COUNTRY AS port_country,
    COUNTRY_CODE AS country_code
FROM cte_raw_ports

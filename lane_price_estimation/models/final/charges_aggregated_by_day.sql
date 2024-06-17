{{ config(materialized='table', 
    schema='final', 
    alias='charges_aggregated',
    pre_hook="{{ pre_hook_counter() }}",
    post_hook=[
        """
        INSERT INTO final.counter (estimation_date, before_lane, before_equipment, before_origin_port, before_dest_port, run_date)
        SELECT 
            subquery.estimation_date, 
            0 AS before_lane,
            0 AS before_equipment,
            0 AS before_origin_port,
            0 AS before_dest_port,
            CURRENT_TIMESTAMP AS run_date
        FROM (
            SELECT 
                DISTINCT estimation_date as estimation_date
            FROM {{ this }}
            WHERE dq_ok = TRUE
        ) AS subquery
        LEFT JOIN final.counter c
        ON subquery.estimation_date = c.estimation_date
        WHERE c.estimation_date IS NULL
        """ ,
        """
        UPDATE final.counter
        SET 
            after_lane = subquery.after_lane,
            after_equipment = subquery.after_equipment,
            after_origin_port = subquery.after_origin_port,
            after_dest_port = subquery.after_dest_port
        FROM (
            SELECT 
                estimation_date,
                COUNT(DISTINCT lane_id) AS after_lane,
                COUNT(DISTINCT equipment_id) AS after_equipment, 
                COUNT(DISTINCT origin_port_id) AS after_origin_port,
                COUNT(DISTINCT destination_port_id) AS after_dest_port
            FROM {{ this }}
            WHERE dq_ok = TRUE
            GROUP BY estimation_date
        ) AS subquery
        WHERE counter.estimation_date = subquery.estimation_date
          AND counter.run_date = (SELECT MAX(run_date) FROM final.counter)
        """]
) }}



WITH daily_aggregate AS (
    SELECT
        estimation_date,
        lane_id,
        equipment_id,
        AVG(total_charge_usd) AS avg_charge_usd,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_charge_usd) AS median_charge_usd
    FROM  {{ ref('datapoint_total_charge_by_day') }}
    GROUP BY estimation_date, lane_id, equipment_id
),

aggregated_with_data_quality AS (
    SELECT
        da.estimation_date,
        da.lane_id,
        CAST(LEFT(da.lane_id, 4) AS INTEGER) AS origin_port_id,
        CAST(RIGHT(da.lane_id, 4) AS INTEGER) AS destination_port_id,
        da.equipment_id,
        da.avg_charge_usd,
        da.median_charge_usd,
        CASE
            WHEN dq.company_coverage >= {{ var("company_coverage_ok") }} AND supplier_coverage >= {{ var("supplier_coverage_ok") }}  THEN True
            ELSE False
        END AS dq_ok


    FROM daily_aggregate da
    JOIN  {{ ref('data_quality_by_coverage') }} dq
     ON da.lane_id          = dq.lane_id
    AND da.equipment_id     = dq.equipment_id
    AND da.estimation_date  = dq.estimation_date

    ORDER BY da.estimation_date, da.lane_id, da.equipment_id

),

agg_add_origin_port AS (
    SELECT 
        agg.estimation_date,
        agg.lane_id,
        agg.equipment_id,
        agg.avg_charge_usd,
        agg.median_charge_usd,
        agg.origin_port_id,
        ps.port_name as origin_port_name,
        ps.region_slug as origin_region_slug,
        agg.destination_port_id,
        agg.dq_ok
    FROM aggregated_with_data_quality agg
    JOIN  {{ ref('stage_ports_model') }} ps
     ON agg.origin_port_id  = ps.port_id
 
),

agg_add_destination_port AS (
    SELECT 
        add.estimation_date,
        add.lane_id,
        add.equipment_id,
        add.avg_charge_usd,
        add.median_charge_usd,
        add.origin_port_id,
        add.origin_port_name,
        add.origin_region_slug,
        add.destination_port_id,
        ps.port_name as destination_port_name,
        ps.region_slug as destination_region_slug,
        add.dq_ok
    FROM agg_add_origin_port add
    JOIN  {{ ref('stage_ports_model') }} ps
     ON add.destination_port_id  = ps.port_id
 
)

SELECT *
FROM agg_add_destination_port

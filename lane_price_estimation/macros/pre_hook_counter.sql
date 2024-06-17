{% macro pre_hook_counter() -%}
    
    {%- if  var("first_run") == "N" -%}
        -- The table exists, run the pre-hook logic
        insert into final.counter (estimation_date, before_lane, before_equipment, before_origin_port, before_dest_port, run_date)
        select 
            estimation_date, 
            COUNT(DISTINCT lane_id) AS before_lane,
            COUNT(DISTINCT equipment_id) AS before_equipment, 
            COUNT(DISTINCT origin_port_id) AS before_origin_port,
            COUNT(DISTINCT destination_port_id) AS before_dest_port,
            CURRENT_TIMESTAMP AS run_date
        FROM {{ this }}
        WHERE dq_ok = TRUE
        GROUP BY estimation_date
    {%- endif -%}
{%- endmacro %}

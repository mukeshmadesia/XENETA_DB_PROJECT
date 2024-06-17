{% macro create_counter_table() -%}

    CREATE TABLE IF NOT EXISTS final.counter (
      estimation_date DATE,
      before_lane INTEGER,
      before_equipment INTEGER,
      before_origin_port INTEGER,
      before_dest_port INTEGER,
      after_lane INTEGER,
      after_equipment INTEGER,
      after_origin_port INTEGER,
      after_dest_port INTEGER,
      run_date TIMESTAMP
    );
{%- endmacro %}
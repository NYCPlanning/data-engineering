WITH all_spatial AS (SELECT * FROM {{ ref("int_spatial__all") }}),

record_details AS (
    SELECT
        source_relation,
        flag_id_field_name,
        variable_type,
        raw_geom IS NOT null AS has_raw_geom,
        lot_geom IS NOT null AS has_lot_geom,
        buffer_geom IS NOT null AS has_buffer_geom
    FROM all_spatial
),

counts AS (
    SELECT
        flag_id_field_name,
        variable_type,
        count(*) AS variable_type_count,
        sum(CASE WHEN has_raw_geom THEN 1 ELSE 0 END)
        AS records_with_raw_geom,
        sum(CASE WHEN has_lot_geom THEN 1 ELSE 0 END)
        AS records_with_lot_geom,
        sum(CASE WHEN has_buffer_geom THEN 1 ELSE 0 END)
        AS records_with_buffer_geom
    FROM record_details
    GROUP BY flag_id_field_name, variable_type
    ORDER BY flag_id_field_name ASC, variable_type ASC
),

final AS (
    SELECT
        flag_id_field_name,
        variable_type,
        variable_type_count,
        records_with_raw_geom,
        records_with_lot_geom,
        records_with_buffer_geom,
        records_with_raw_geom - records_with_lot_geom AS records_without_lots
    FROM counts
)

SELECT * FROM final;

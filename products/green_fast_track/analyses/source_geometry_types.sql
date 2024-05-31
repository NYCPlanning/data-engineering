WITH all_spatial AS (SELECT * FROM {{ ref("int_spatial__all") }}),

record_details AS (
    SELECT
        source_relation,
        flag_id_field_name,
        variable_type,
        geometrytype(raw_geom) AS raw_geom_type
    FROM all_spatial
)

SELECT
    flag_id_field_name,
    variable_type,
    raw_geom_type,
    count(*) AS raw_geom_type_count
FROM record_details
GROUP BY flag_id_field_name, variable_type, raw_geom_type
ORDER BY flag_id_field_name ASC, variable_type ASC, raw_geom_type ASC;

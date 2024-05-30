WITH all_spatial AS (SELECT * FROM {{ ref("int_spatial__all") }}),

record_details AS (
    SELECT
        source_relation,
        flag_id_field_name,
        variable_type,
        geometrytype(variable_geom) AS geometry_type
    FROM all_spatial
)

SELECT
    flag_id_field_name,
    variable_type,
    geometry_type,
    count(*) AS geometry_type_count
FROM record_details
GROUP BY flag_id_field_name, variable_type, geometry_type
ORDER BY flag_id_field_name, variable_type ASC, geometry_type ASC;

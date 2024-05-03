DROP TABLE IF EXISTS {{ table_name }};
WITH boundaries AS (
    SELECT * FROM cpdb_adminbounds
    WHERE
        cpdb_adminbounds.admin_boundary_type = '{{ geography_type }}'
        AND cpdb_adminbounds.admin_boundary_id = '{{ geography_id }}'
),

all_projects AS (
    SELECT * FROM cpdb_projects_shp
),

final AS (
    SELECT
        '{{ geography_name }}' AS geography_name,
        '{{ geography_id }}' AS geography_id,
        all_projects.*
    FROM all_projects
    INNER JOIN boundaries ON all_projects.maprojid = boundaries.feature_id
)
SELECT * INTO {{ table_name }}
FROM final
ORDER BY maprojid ASC;

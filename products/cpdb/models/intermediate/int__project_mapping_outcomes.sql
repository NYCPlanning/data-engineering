-- one row per project: where its geometry came from and whether it's in the product
WITH other_methods AS (
    SELECT DISTINCT c.maprojid
    FROM {{ ref('int__project_geom_candidates') }} AS c
    INNER JOIN {{ ref('int__project_geom') }} AS g
        ON c.maprojid = g.maprojid AND c.method != g.method
),

commitments AS (
    SELECT
        maprojid,
        COUNT(*) AS planned_commitment_records
    FROM {{ ref('int__ccp_commitments') }}
    GROUP BY maprojid
)

SELECT
    p.maprojid,
    p.magency,
    p.magencyacro,
    p.plannedcommit_total,
    cm.planned_commitment_records,
    g.method AS geometry_method,
    g.candidate AS geometry_source,
    c.priority AS geometry_source_priority,
    g.geom_removed_reason,
    o.maprojid IS NOT NULL AS also_found_by_another_method,
    CASE ST_GEOMETRYTYPE(d.geom)
        WHEN 'ST_MultiPoint' THEN 'points'
        WHEN 'ST_MultiPolygon' THEN 'polygons'
    END AS mapped_as
FROM {{ ref('int__ccp_projects') }} AS p
INNER JOIN {{ ref('int__project_geom') }} AS g ON p.maprojid = g.maprojid
INNER JOIN {{ ref('cpdb_dcpattributes') }} AS d ON p.maprojid = d.maprojid
LEFT JOIN {{ ref('int__project_geom_candidates') }} AS c
    ON g.maprojid = c.maprojid AND g.candidate = c.candidate
LEFT JOIN other_methods AS o ON p.maprojid = o.maprojid
INNER JOIN commitments AS cm ON p.maprojid = cm.maprojid

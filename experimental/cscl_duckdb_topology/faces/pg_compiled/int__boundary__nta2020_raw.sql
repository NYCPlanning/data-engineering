

-- One row per 2020 NTA, from its exterior edges, before noise-ring removal.

WITH faces AS (
    SELECT
        entity_id,
        (st_dump(st_polygonize(geom))).geom AS face
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__boundary__nta2020_edges"
    WHERE edge_type = 'exterior'
    GROUP BY entity_id
),

member_points AS (
    SELECT
        m.nta2020 AS entity_id,
        st_pointonsurface(a.geom) AS pt
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."int__topology__ap_entities" AS m
    INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__atomicpolygons" AS a ON m.atomicid = a.atomicid
    WHERE
        m.nta2020 IS NOT NULL
        AND m.water_flag != '1'
),

kept AS (
    SELECT
        f.entity_id,
        f.face
    FROM faces AS f
    WHERE EXISTS (
        SELECT 1 FROM member_points AS mp
        WHERE mp.entity_id = f.entity_id AND f.face && mp.pt AND st_contains(f.face, mp.pt)
    )
)

-- kept faces share exact, already-noded edges, so this union only dissolves them
SELECT
    entity_id,
    st_union(face) AS geom
FROM kept
GROUP BY entity_id

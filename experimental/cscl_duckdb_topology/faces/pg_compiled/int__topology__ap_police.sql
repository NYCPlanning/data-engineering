

-- Police geography of every AtomicPolygon - precinct, patrol borough and sector (NYPD
-- beat) - computed once here and shared, so ThinLION and anything else reading an AP's
-- police attributes can't disagree.
--
-- The rule reproduces prod's own per-AP assignment (verified against the published
-- ThinLION: 69,782 of 69,786 APs match on all three fields):
--   1. If the AP's centroid lies inside the AP (68,204 APs), use the polygon containing
--      the centroid - an exact match with prod for every one of them.
--   2. Otherwise - crescent, concave or multipart APs whose centroid falls outside them
--      (1,582 APs) - use the value holding the largest share of the AP's area (summed per
--      value: the beat layer has several polygons per sector). Here PostGIS's and the
--      legacy ETL's fallback points disagree, and majority area is what matches prod.
-- The 4 remaining mismatches are near-ties or slivers (e.g. 1024000006: 51% / 49%).
--
-- NYPD's polygons don't follow AP edges (their lines cut through hundreds of APs), which
-- is why the choice of rule matters at all. method / *_share record how each AP was
-- assigned. NULL where an AP touches no polygon of that layer.



WITH aps AS (
    SELECT
        atomicid,
        geom,
        st_centroid(geom) AS centroid,
        st_within(st_centroid(geom), geom) AS centroid_inside
    FROM "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__atomicpolygons"
),


    precinct_at_centroid AS (
        SELECT DISTINCT ON (a.atomicid)
            a.atomicid,
            p.precinct AS value,
            p.globalid
        FROM aps AS a
        INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__nypdprecinct" AS p ON st_intersects(p.geom, a.centroid)
        WHERE a.centroid_inside
        ORDER BY a.atomicid, p.globalid
    ),

    precinct_overlap AS (
        -- only APs whose centroid falls outside them; an AP wholly inside one polygon skips
        -- the intersection
        SELECT
            a.atomicid,
            p.precinct AS value,
            p.globalid,
            CASE
                WHEN st_covers(p.geom, a.geom) THEN st_area(a.geom)
                ELSE st_area(st_intersection(a.geom, p.geom))
            END AS overlap_sqft
        FROM aps AS a
        INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__nypdprecinct" AS p ON st_intersects(a.geom, p.geom)
        WHERE NOT a.centroid_inside
    ),

    precinct_by_value AS (
        SELECT
            atomicid,
            value,
            sum(overlap_sqft) AS value_sqft,
            (array_agg(globalid ORDER BY overlap_sqft DESC))[1] AS globalid
        FROM precinct_overlap
        WHERE overlap_sqft > 0
        GROUP BY atomicid, value
    ),

    precinct_majority AS (
        SELECT DISTINCT ON (atomicid)
            atomicid,
            value,
            globalid,
            value_sqft / nullif(sum(value_sqft) OVER (PARTITION BY atomicid), 0) AS share
        FROM precinct_by_value
        ORDER BY atomicid ASC, value_sqft DESC, value ASC
    ),


    patrol_borough_at_centroid AS (
        SELECT DISTINCT ON (a.atomicid)
            a.atomicid,
            p.patrol_borough AS value,
            p.globalid
        FROM aps AS a
        INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__nypdpatrolborough" AS p ON st_intersects(p.geom, a.centroid)
        WHERE a.centroid_inside
        ORDER BY a.atomicid, p.globalid
    ),

    patrol_borough_overlap AS (
        -- only APs whose centroid falls outside them; an AP wholly inside one polygon skips
        -- the intersection
        SELECT
            a.atomicid,
            p.patrol_borough AS value,
            p.globalid,
            CASE
                WHEN st_covers(p.geom, a.geom) THEN st_area(a.geom)
                ELSE st_area(st_intersection(a.geom, p.geom))
            END AS overlap_sqft
        FROM aps AS a
        INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__nypdpatrolborough" AS p ON st_intersects(a.geom, p.geom)
        WHERE NOT a.centroid_inside
    ),

    patrol_borough_by_value AS (
        SELECT
            atomicid,
            value,
            sum(overlap_sqft) AS value_sqft,
            (array_agg(globalid ORDER BY overlap_sqft DESC))[1] AS globalid
        FROM patrol_borough_overlap
        WHERE overlap_sqft > 0
        GROUP BY atomicid, value
    ),

    patrol_borough_majority AS (
        SELECT DISTINCT ON (atomicid)
            atomicid,
            value,
            globalid,
            value_sqft / nullif(sum(value_sqft) OVER (PARTITION BY atomicid), 0) AS share
        FROM patrol_borough_by_value
        ORDER BY atomicid ASC, value_sqft DESC, value ASC
    ),


    sector_at_centroid AS (
        SELECT DISTINCT ON (a.atomicid)
            a.atomicid,
            p.sector AS value,
            p.globalid
        FROM aps AS a
        INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__nypdbeat" AS p ON st_intersects(p.geom, a.centroid)
        WHERE a.centroid_inside
        ORDER BY a.atomicid, p.globalid
    ),

    sector_overlap AS (
        -- only APs whose centroid falls outside them; an AP wholly inside one polygon skips
        -- the intersection
        SELECT
            a.atomicid,
            p.sector AS value,
            p.globalid,
            CASE
                WHEN st_covers(p.geom, a.geom) THEN st_area(a.geom)
                ELSE st_area(st_intersection(a.geom, p.geom))
            END AS overlap_sqft
        FROM aps AS a
        INNER JOIN "db-cscl"."ar_cscl_districts_gdb_db_qa_oct4"."stg__nypdbeat" AS p ON st_intersects(a.geom, p.geom)
        WHERE NOT a.centroid_inside
    ),

    sector_by_value AS (
        SELECT
            atomicid,
            value,
            sum(overlap_sqft) AS value_sqft,
            (array_agg(globalid ORDER BY overlap_sqft DESC))[1] AS globalid
        FROM sector_overlap
        WHERE overlap_sqft > 0
        GROUP BY atomicid, value
    ),

    sector_majority AS (
        SELECT DISTINCT ON (atomicid)
            atomicid,
            value,
            globalid,
            value_sqft / nullif(sum(value_sqft) OVER (PARTITION BY atomicid), 0) AS share
        FROM sector_by_value
        ORDER BY atomicid ASC, value_sqft DESC, value ASC
    )


SELECT
    a.atomicid,
    CASE WHEN a.centroid_inside THEN 'centroid' ELSE 'majority_area' END AS method,

    coalesce(precinct_at_centroid.value, precinct_majority.value) AS police_precinct,
    round(precinct_majority.share::numeric, 4) AS police_precinct_share,
    coalesce(precinct_at_centroid.globalid, precinct_majority.globalid) AS nypdprecinct_globalid,

    coalesce(patrol_borough_at_centroid.value, patrol_borough_majority.value) AS patrol_borough,
    round(patrol_borough_majority.share::numeric, 4) AS patrol_borough_share,
    coalesce(patrol_borough_at_centroid.globalid, patrol_borough_majority.globalid) AS nypdpatrolborough_globalid,

    coalesce(sector_at_centroid.value, sector_majority.value) AS police_sector,
    round(sector_majority.share::numeric, 4) AS police_sector_share,
    coalesce(sector_at_centroid.globalid, sector_majority.globalid) AS nypdbeat_globalid

FROM aps AS a

    LEFT JOIN precinct_at_centroid ON a.atomicid = precinct_at_centroid.atomicid
    LEFT JOIN precinct_majority ON a.atomicid = precinct_majority.atomicid

    LEFT JOIN patrol_borough_at_centroid ON a.atomicid = patrol_borough_at_centroid.atomicid
    LEFT JOIN patrol_borough_majority ON a.atomicid = patrol_borough_majority.atomicid

    LEFT JOIN sector_at_centroid ON a.atomicid = sector_at_centroid.atomicid
    LEFT JOIN sector_majority ON a.atomicid = sector_majority.atomicid

-- Correctness vs PG's tables for the same layer ({name}), all compared inside DuckDB.
CREATE OR REPLACE TABLE {name}_cmp_edges AS
WITH d AS (SELECT entity_id, node_lo, node_hi, is_arc, edge_type FROM {name}_edges),
p AS (SELECT entity_id, node_lo, node_hi, is_arc, edge_type FROM read_parquet('{data}/pg_{name}_edges.parquet'))
SELECT
    (SELECT count(*) FROM d) AS duck_rows,
    (SELECT count(*) FROM p) AS pg_rows,
    (SELECT count(*) FROM (SELECT * FROM d EXCEPT ALL SELECT * FROM p)) AS duck_only,
    (SELECT count(*) FROM (SELECT * FROM p EXCEPT ALL SELECT * FROM d)) AS pg_only;

CREATE OR REPLACE TABLE {name}_cmp_validity AS
WITH p AS (SELECT * FROM read_parquet('{data}/pg_{name}_validity.parquet'))
SELECT
    count(*) FILTER (WHERE d.entity_id IS NULL) AS pg_only,
    count(*) FILTER (WHERE p.entity_id IS NULL) AS duck_only,
    count(*) FILTER (WHERE d.is_valid) AS duck_valid,
    count(*) FILTER (WHERE p.is_valid) AS pg_valid,
    count(*) FILTER (
        WHERE (d.is_valid, d.even_degree_ok, d.n_odd_degree_nodes, d.n_nodes_total, d.is_simple_ok,
               d.buildarea_ok, d.final_geom_valid_ok)
           IS DISTINCT FROM
              (p.is_valid, p.even_degree_ok, p.n_odd_degree_nodes, p.n_nodes_total, p.is_simple_ok,
               p.buildarea_ok, p.final_geom_valid_ok)
    ) AS flag_mismatches
FROM {name}_validity AS d
FULL OUTER JOIN p ON d.entity_id = p.entity_id;

CREATE OR REPLACE TABLE {name}_cmp_geom_detail AS
WITH j AS (
    SELECT
        coalesce(d.entity_id, p.entity_id) AS entity_id,
        d.geom AS dg, p.geom AS pg_geom, d.layer
    FROM (
        SELECT entity_id, geom, 'raw' AS layer FROM {name}_raw
        UNION ALL SELECT entity_id, geom, 'final' FROM {name}
    ) AS d
    FULL OUTER JOIN (
        SELECT entity_id, geom, 'raw' AS layer FROM read_parquet('{data}/pg_{name}_raw.parquet')
        UNION ALL SELECT entity_id, geom, 'final' FROM read_parquet('{data}/pg_{name}.parquet')
    ) AS p ON d.entity_id = p.entity_id AND d.layer = p.layer
)
SELECT
    entity_id,
    layer,
    dg IS NOT NULL AS in_duck,
    pg_geom IS NOT NULL AS in_pg,
    ST_IsValid(dg) AS duck_valid,
    ST_IsValid(pg_geom) AS pg_valid,
    ST_Equals(dg, pg_geom) AS equals,
    ST_NumGeometries(dg) = ST_NumGeometries(pg_geom) AS same_parts,
    ST_NPoints(dg) = ST_NPoints(pg_geom) AS same_npoints,
    ST_Area(dg) - ST_Area(pg_geom) AS area_diff,
    ST_Area(ST_SymDifference(dg, pg_geom)) AS symdiff_area,
    ST_Area(pg_geom) AS pg_area
FROM j;

CREATE OR REPLACE TABLE {name}_cmp_geom AS
SELECT
    layer,
    count(*) FILTER (WHERE in_duck AND in_pg) AS matched_ids,
    count(*) FILTER (WHERE NOT in_pg) AS duck_only_ids,
    count(*) FILTER (WHERE NOT in_duck) AS pg_only_ids,
    count(*) FILTER (WHERE duck_valid) AS duck_st_isvalid,
    count(*) FILTER (WHERE pg_valid) AS pg_st_isvalid,
    count(*) FILTER (WHERE equals) AS st_equals,
    count(*) FILTER (WHERE same_parts) AS same_part_count,
    count(*) FILTER (WHERE same_npoints) AS same_npoints,
    max(abs(area_diff)) AS max_abs_area_diff_sqft,
    sum(symdiff_area) AS total_symdiff_sqft,
    max(symdiff_area) AS max_symdiff_sqft,
    count(*) FILTER (WHERE symdiff_area > 0.01) AS n_symdiff_gt_001sqft,
    sum(pg_area) AS total_pg_area_sqft
FROM {name}_cmp_geom_detail
GROUP BY layer
ORDER BY layer DESC;

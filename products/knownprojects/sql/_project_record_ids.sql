/*
DESCRIPTION:
    Create initial table of project inputs (groups of record_ids that refer to the
    same project). Match non-DOB records based on spatial overlap,
    forming arrays of individual record_ids which get called
    project_record_ids. Two of the neighborhood study sources are not
    included, as units from these sources do not deduplicate
    with other sources.

INPUTS:
	combined
OUTPUTS:
    _project_record_ids
*/

-- Identify spatially overlapping non-DOB records
DROP TABLE IF EXISTS _project_record_ids;
WITH
dbscan AS (
    SELECT
        record_id,
        geom,
        ST_ClusterDBSCAN(geom, 0, 1) OVER () AS id
    FROM combined
    WHERE
        source NOT IN (
            'DOB',
            'Neighborhood Study Rezoning Commitments',
            'Future Neighborhood Studies'
        )
),

project_record_join AS (
    SELECT
        record_id,
        id,
        (geom IS NULL)::integer AS no_geom,
        geom,
        count(record_id) OVER (PARTITION BY id) AS records_in_project
    FROM dbscan
),

all_intersections AS (
    SELECT
        a.id,
        ST_Union(ST_Intersection(a.geom, b.geom)) AS intersect_geom
    FROM project_record_join AS a, project_record_join AS b
    WHERE
        a.record_id < b.record_id
        AND a.records_in_project > 1
        AND b.records_in_project > 1
        AND a.no_geom = 0
        AND b.no_geom = 0
        AND a.id = b.id
    GROUP BY a.id
)

/*
this last query would determine whether the cluster identified by
dbscan would stay together as a cluster or de-clustered by checking
the intersecting geometries is at least one of these three cases:
a) overlapping with record geometry (St_Overlaps)
b) falling completely within the record geometry (St_Within)
c) containing the record geometry completely (St_Contains)
with the geometries of the record geometries.
*/
SELECT array_agg(a.record_id) AS project_record_ids
INTO _project_record_ids
FROM project_record_join AS a, all_intersections AS b
WHERE
    (
        ST_Overlaps(a.geom, b.intersect_geom)
        OR ST_Within(b.intersect_geom, a.geom)
        OR ST_Contains(b.intersect_geom, a.geom)
    )
    AND a.id IS NOT NULL
    AND a.id = b.id
GROUP BY a.id;

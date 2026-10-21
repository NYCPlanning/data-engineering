{{ config(materialized='table', tags=['qa']) }}

-- The symmetric difference of each district present on both sides and not equal vertex
-- for vertex, as single polygons. side: 'added' if the piece is in our district and not
-- prod's, 'removed' if the reverse (decided by the piece's interior point).
--
-- is_noise: mean width (2 * area / perimeter, exact for a long thin sliver) under
-- district_review_noise_width_ft. The source's coordinates sit on a 2^-14 ft (~6.1e-5 ft)
-- grid, and two copies of the same corner differ by up to 0.0003 ft (see
-- ap_topology_node_merge_tolerance_ft), so slivers between two copies of the same line
-- are a few grid cells wide at most. 0.001 ft is about 16 cells, 3x that worst case, and
-- still far below anything a reviewer could see.

WITH pairs AS (
    SELECT
        layer,
        district_key,
        st_makevalid(geom_new) AS a,
        geom_prior AS e
    FROM {{ ref('qa__district_review_pairs') }}
    WHERE geom_new IS NOT NULL AND geom_prior IS NOT NULL AND NOT is_equal
),

pieces AS (
    SELECT
        layer,
        district_key,
        a,
        unnest(st_dump(st_collectionextract(st_symdifference(a, e), 3))).geom AS geom
    FROM pairs
),

measured AS (
    SELECT
        layer,
        district_key,
        CASE WHEN st_intersects(st_pointonsurface(geom), a) THEN 'added' ELSE 'removed' END AS side,
        st_area(geom) AS area_sqft,
        st_perimeter(geom) AS perimeter_ft,
        2 * st_area(geom) / nullif(st_perimeter(geom), 0) AS mean_width_ft,
        geom
    FROM pieces
    WHERE NOT st_isempty(geom)
)

-- Noise pieces are kept (they tell identical from grid_noise) but without geometry:
-- there are tens of thousands per layer and nothing reads their shapes.
SELECT
    * EXCLUDE (geom, is_noise),
    is_noise,
    CASE WHEN NOT is_noise THEN geom END AS geom
FROM (
    SELECT
        *,
        coalesce(mean_width_ft < {{ var('district_review_noise_width_ft') }}, TRUE) AS is_noise
    FROM measured
) AS flagged

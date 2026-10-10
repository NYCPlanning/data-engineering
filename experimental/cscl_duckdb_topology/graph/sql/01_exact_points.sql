-- int__topology__exact_points: every distinct non-arc-mid vertex coordinate. PG groups by
-- the geometry; grouping by its exact x/y doubles is the same equivalence.
SELECT
    row_number() OVER (ORDER BY x, y) AS point_id,
    x,
    y,
    st_point(x, y) AS geom,
    vertex_count
FROM (
    SELECT x, y, count(*) AS vertex_count
    FROM vertices
    WHERE NOT is_arc_mid
    GROUP BY x, y
)

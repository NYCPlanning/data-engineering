-- int__topology__grid_cells. PG's round(double) rounds half to even (rint); DuckDB's
-- round() rounds half away from zero, so round_even() is the faithful spelling.
SELECT
    round_even(x / ${tol}, 0)::bigint AS x_cell,
    round_even(y / ${tol}, 0)::bigint AS y_cell,
    arg_min(geom, point_id) AS geom,
    list(point_id ORDER BY point_id) AS exact_point_ids
FROM exact_points
GROUP BY ALL

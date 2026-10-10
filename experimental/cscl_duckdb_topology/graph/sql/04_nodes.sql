-- int__topology__nodes: representative coordinate = first exact point by id.
SELECT
    p.node_id,
    arg_min(ec.x, p.exact_point_id) AS x,
    arg_min(ec.y, p.exact_point_id) AS y,
    arg_min(ec.geom, p.exact_point_id) AS geom
FROM point_to_node AS p
INNER JOIN exact_points AS ec ON p.exact_point_id = ec.point_id
GROUP BY p.node_id

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['node_id'], 'unique': True},
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Canonical topology nodes, one per int__topology__point_to_node cluster. The
-- representative coordinate (first exact point by id) is arbitrary - all that matters
-- is that every member of a cluster resolves to the same one.

SELECT
    p.node_id,
    (array_agg(ec.geom ORDER BY p.exact_point_id))[1] AS geom
FROM {{ ref('int__topology__point_to_node') }} AS p
INNER JOIN {{ ref('int__topology__exact_points') }} AS ec ON p.exact_point_id = ec.point_id
GROUP BY p.node_id

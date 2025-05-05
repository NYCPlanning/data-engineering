{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH segment_end_nodes_pivoted AS (
    SELECT
        segmentid,
        from_nodeid,
        to_nodeid
    FROM {{ ref("int__centerline_segments_with_nodes") }}
    WHERE from_nodeid IS NOT NULL AND to_nodeid IS NOT NULL
),
segment_end_nodes_unpivoted AS (
    SELECT 
        segmentid,
        from_nodeid AS node
    FROM segment_end_nodes_pivoted
    UNION
    SELECT
        segmentid,
        to_nodeid as node
    FROM segment_end_nodes_pivoted
)
SELECT
    segment_to_node.segmentid,
    segment_to_node.node,
    node_to_segment.segmentid AS to_segment,
    CASE
        WHEN node_to_segment.segmentid IS NULL THEN 1 ELSE 0
    END AS node_missing_neighbor
FROM segment_end_nodes_unpivoted AS segment_to_node
LEFT JOIN segment_end_nodes_unpivoted AS node_to_segment
ON segment_to_node.node = node_to_segment.node AND segment_to_node.segmentid <> node_to_segment.segmentid

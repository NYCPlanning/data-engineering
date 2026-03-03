WITH lion AS (
    SELECT DISTINCT ON (segmentid)
        segmentid,
        segment_type,
        from_nodeid,
        to_nodeid,
        geom,
        midpoint
    FROM {{ ref("int__lion") }}
    ORDER BY segmentid
),

cscl_rpl AS (SELECT * FROM {{ source("recipe_sources", "dcp_cscl_roadbed_pointer_list") }}),

generic_attributes AS (
    SELECT
        cscl_rpl.generic_segmentid,
        lion.segment_type AS generic_segmenttype,
        cscl_rpl.roadbed_segmentid,
        cscl_rpl.roadbed_position_code,
        'B' AS node_correspondence_indicator, -- all records are 'B' in production, but the docs say it should be node_correspondence_ind
        -- cscl_rpl.node_correspondence_ind AS node_correspondence_indicator,
        chr(64 + cscl_rpl.from_node_level_coincident_rb) AS from_node_level_code_of_coincident_roadbed_segment,
        chr(64 + cscl_rpl.to_node_level_coincident_rb) AS to_node_level_code_of_coincident_roadbed_segment,
        lion.from_nodeid AS from_nodeid_of_generic_segment,
        lion.to_nodeid AS to_nodeid_of_generic_segment,
        lion.geom AS generic_geom
    FROM cscl_rpl
    LEFT JOIN lion
        ON cscl_rpl.generic_segmentid = lion.segmentid
),

roadbed_attributes AS (
    SELECT
        generic_attributes.*,
        lion.from_nodeid AS from_nodeid_of_roadbed_segment,
        lion.to_nodeid AS to_nodeid_of_roadbed_segment,
        lion.geom,
        lion.midpoint
    FROM generic_attributes
    LEFT JOIN lion
        ON generic_attributes.roadbed_segmentid = lion.segmentid
),

-- Compute the cross product for every row and attach the R outermost's cross product
-- as a per-group sign reference. Comparing signs (rather than using the raw sign of
-- the cross product alone) means side determination is correct regardless of the
-- direction of the generic segment geometry.
side_reference AS (
    SELECT
        *,
        (
            (st_x(st_endpoint(generic_geom)) - st_x(st_startpoint(generic_geom)))
            * (st_y(midpoint) - st_y(st_startpoint(generic_geom)))
            - (st_y(st_endpoint(generic_geom)) - st_y(st_startpoint(generic_geom)))
            * (st_x(midpoint) - st_x(st_startpoint(generic_geom)))
        ) AS cross_product,
        first_value(
            (st_x(st_endpoint(generic_geom)) - st_x(st_startpoint(generic_geom)))
            * (st_y(midpoint) - st_y(st_startpoint(generic_geom)))
            - (st_y(st_endpoint(generic_geom)) - st_y(st_startpoint(generic_geom)))
            * (st_x(midpoint) - st_x(st_startpoint(generic_geom)))
        ) OVER (
            PARTITION BY generic_segmentid
            ORDER BY (roadbed_position_code = 'R') DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS r_cross_product
    FROM roadbed_attributes
),

-- Ordering:
--   1. 'R' outermost
--   2. Right-side 'I' segments, from outermost to innermost
--   3. 'L' outermost
--   4. Left-side 'I' segments, from outermost to innermost
--
-- For 'I' segments, side is determined by whether the row's cross product has the
-- same sign as the R outermost's cross product for that generic segment.
-- Within a side, rows are ordered by descending perpendicular distance from the
-- generic segment (outermost first).
add_group_order_id AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY generic_segmentid
            ORDER BY
                CASE
                    WHEN roadbed_position_code = 'R' THEN 1
                    WHEN
                        roadbed_position_code = 'I'
                        AND sign(cross_product) = sign(r_cross_product) THEN 2
                    WHEN roadbed_position_code = 'L' THEN 3
                    ELSE 4  -- left-side 'I' segments
                END,
                -- Negate so outermost (largest perpendicular distance) sorts first
                CASE
                    WHEN roadbed_position_code = 'I'
                        THEN -st_distance(midpoint, generic_geom)
                    ELSE 0
                END
        ) AS group_order_id
    FROM side_reference
),

final AS (
    SELECT
        generic_segmentid || '_' || group_order_id AS rpl_id,
        generic_segmentid,
        generic_segmenttype,
        roadbed_segmentid,
        group_order_id,
        roadbed_position_code,
        node_correspondence_indicator,
        from_node_level_code_of_coincident_roadbed_segment,
        to_node_level_code_of_coincident_roadbed_segment,
        from_nodeid_of_roadbed_segment,
        from_nodeid_of_generic_segment,
        to_nodeid_of_roadbed_segment,
        to_nodeid_of_generic_segment,
        midpoint,
        geom
    FROM add_group_order_id
    ORDER BY rpl_id
)

SELECT * FROM final

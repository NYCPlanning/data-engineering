WITH lion AS (
    -- A segmentid can have more than one int__lion row (a centerline record plus one or
    -- more altsegmentdata protosegments), and some of those protosegments are reversed
    -- relative to the centerline - i.e. same endpoints, opposite from/to direction. Without
    -- a tiebreak here, DISTINCT ON picks whichever row scan order happens to surface first,
    -- so from/to nodeids could flip nondeterministically between runs. Prefer the
    -- canonically-directed centerline row when one exists.
    SELECT DISTINCT ON (segmentid)
        segmentid,
        segment_type,
        from_nodeid,
        to_nodeid,
        geom,
        midpoint
    FROM {{ ref("int__lion") }}
    ORDER BY segmentid, (source_table = 'centerline') DESC
),

-- Prod drops a row whose generic or roadbed segment matches no centerline
-- (CSCL-LOG-RPL-06, -05, logged in log__rpl_missing_roadbed under the same conditions).
centerline AS (
    SELECT DISTINCT segmentid
    FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
    WHERE geom IS NOT NULL AND NOT st_isempty(geom)
),

cscl_rpl AS (
    SELECT rpl.*
    FROM {{ source("recipe_sources", "dcp_cscl_roadbed_pointer_list") }} AS rpl
    WHERE
        EXISTS (
            SELECT 1 FROM centerline
            WHERE centerline.segmentid = rpl.generic_segmentid
        )
        AND EXISTS (
            SELECT 1 FROM centerline
            WHERE centerline.segmentid = rpl.roadbed_segmentid
        )
),

generic_attributes AS (
    SELECT
        cscl_rpl.globalid,
        cscl_rpl.ogc_fid,
        cscl_rpl.generic_segmentid,
        lion.segment_type AS generic_segmenttype,
        cscl_rpl.roadbed_segmentid,
        cscl_rpl.roadbed_position_code,
        'B' AS node_correspondence_indicator, -- all records are 'B' in production, but the docs say it should be node_correspondence_ind
        -- cscl_rpl.node_correspondence_ind AS node_correspondence_indicator,
        -- 1-26 is A-Z and 99 is '*'; any other number is blank, like prod (CSCL-LOG-RPL-08)
        convert_level_code(cscl_rpl.from_node_level_coincident_rb, 'centerline')
            AS from_node_level_code_of_coincident_roadbed_segment,
        convert_level_code(cscl_rpl.to_node_level_coincident_rb, 'centerline')
            AS to_node_level_code_of_coincident_roadbed_segment,
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

-- Cross product of the generic segment's direction with the roadbed's midpoint: negative
-- when the roadbed lies to the right of the generic as digitized (prod's
-- IsPointOnRightOfLine).
cross_products AS (
    SELECT
        *,
        (
            (st_x(st_endpoint(generic_geom)) - st_x(st_startpoint(generic_geom)))
            * (st_y(midpoint) - st_y(st_startpoint(generic_geom)))
            - (st_y(st_endpoint(generic_geom)) - st_y(st_startpoint(generic_geom)))
            * (st_x(midpoint) - st_x(st_startpoint(generic_geom)))
        ) AS cross_product
    FROM roadbed_attributes
),

-- Attach the R outermost's cross product as a per-group sign reference. Comparing signs
-- (rather than using the raw sign of the cross product alone) means side determination is
-- correct regardless of the direction of the generic segment geometry. A generic with no
-- R roadbed falls back to -1, prod's "right of the generic as digitized": prod lists
-- right-side roadbeds first, then left-side ones.
side_reference AS (
    SELECT
        *,
        coalesce(
            max(cross_product) FILTER (WHERE roadbed_position_code = 'R') OVER (
                PARTITION BY generic_segmentid
            ),
            -1
        ) AS r_cross_product
    FROM cross_products
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
-- generic segment (outermost first). Ties (e.g. stacked roadbeds with the same midpoint)
-- go by ogc_fid, the source table's OBJECTID: prod's stable sort keeps them in cursor
-- order.
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
                END,
                ogc_fid
        ) AS group_order_id
    FROM side_reference
),

final AS (
    SELECT
        generic_segmentid || '_' || group_order_id AS rpl_id,
        globalid,
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

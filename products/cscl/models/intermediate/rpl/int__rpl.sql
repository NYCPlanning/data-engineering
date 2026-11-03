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

-- The spec's order (var fix_rpl_roadbed_order on, CSCL-RPL-02). Cross product of the
-- generic segment's direction with the roadbed's midpoint: negative when the roadbed lies
-- to the right of the generic's chord as digitized.
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
spec_order AS (
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
        ) AS spec_group_order_id
    FROM side_reference
),

-- Prod's order (the default, CSCL-RPL-02; RPLExtractClass.cs:172-190 and 390-436).
-- Prod puts each roadbed in a right or a left list by a geometric side test alone, then
-- sorts each list by distance, whatever the roadbed's ROADBED_POSITION_CODE.
--
-- Side (IsPointOnRightOfLine, CSCLArcObjectslHelperMethods.cs:84-99): the side of the
-- generic, as digitized, that the roadbed's midpoint is on, at the generic's nearest
-- point to it (QueryPointAndDistance, no extension). Here: the generic step nearest the
-- midpoint (the first, on a tie), right when the cross product is negative. Circular arcs
-- are their densified steps.
generic_steps AS (
    SELECT
        globalid,
        midpoint,
        unnest(generate_series(1, st_npoints(generic_geom) - 1)) AS step,
        generic_geom
    FROM roadbed_attributes
),

step_points AS (
    SELECT
        globalid,
        midpoint,
        step,
        st_pointn(generic_geom, step::INTEGER) AS step_start,
        st_pointn(generic_geom, step::INTEGER + 1) AS step_end
    FROM generic_steps
),

nearest_step AS (
    SELECT
        globalid,
        arg_min(
            {
                'ax': st_x(step_start), 'ay': st_y(step_start),
                'bx': st_x(step_end), 'by': st_y(step_end)
            },
            (st_distance(st_makeline(step_start, step_end), midpoint), step)
        ) AS s,
        any_value(midpoint) AS midpoint
    FROM step_points
    GROUP BY globalid
),

roadbed_side AS (
    SELECT
        globalid,
        (s.bx - s.ax) * (st_y(midpoint) - s.ay)
        - (st_x(midpoint) - s.ax) * (s.by - s.ay) < 0 AS right_side
    FROM nearest_step
),

-- Distance (GetDistanceBetweenLines, CSCLArcObjectslHelperMethods.cs:117-163): a 0.1 ft
-- normal from the generic's midpoint, toward the roadbed's side, extended to meet the
-- roadbed itself (ConstructExtended: the roadbed isn't extended), and its length. An
-- extension from the far end (the roadbed lies behind the normal) adds the 0.1 ft. Where
-- the perpendicular misses the roadbed, the normal keeps its 0.1 ft, so the roadbed sorts
-- last on its side.
generic_steps_along AS (
    SELECT
        generic_segmentid,
        generic_geom,
        step,
        st_pointn(generic_geom, step::INTEGER) AS step_start,
        st_pointn(generic_geom, step::INTEGER + 1) AS step_end
    FROM (
        SELECT DISTINCT
            generic_segmentid,
            generic_geom,
            unnest(generate_series(1, st_npoints(generic_geom) - 1)) AS step
        FROM roadbed_attributes
    ) AS distinct_steps
),

generic_step_lengths AS (
    SELECT
        generic_segmentid,
        step,
        st_x(st_lineinterpolatepoint(generic_geom, 0.5)) AS mx,
        st_y(st_lineinterpolatepoint(generic_geom, 0.5)) AS my,
        (st_x(step_end) - st_x(step_start)) / st_distance(step_start, step_end) AS dx,
        (st_y(step_end) - st_y(step_start)) / st_distance(step_start, step_end) AS dy,
        sum(st_distance(step_start, step_end)) OVER (
            PARTITION BY generic_segmentid ORDER BY step
        ) >= st_length(generic_geom) / 2 AS reaches_midpoint
    FROM generic_steps_along
    WHERE st_distance(step_start, step_end) > 0
),

-- the generic's direction at its midpoint: the first step that reaches it
generic_midpoint_step AS (
    SELECT
        generic_segmentid,
        mx,
        my,
        dx,
        dy
    FROM generic_step_lengths
    WHERE reaches_midpoint
    QUALIFY row_number() OVER (PARTITION BY generic_segmentid ORDER BY step) = 1
),

-- The perpendicular through the generic's midpoint, as a long line, with u the unit
-- normal toward the roadbed's side (right of (dx, dy) is (dy, -dx)).
perpendiculars AS (
    SELECT
        roadbed_attributes.globalid,
        roadbed_attributes.geom,
        generic_midpoint_step.mx,
        generic_midpoint_step.my,
        CASE
            WHEN roadbed_side.right_side THEN generic_midpoint_step.dy
            ELSE -generic_midpoint_step.dy
        END AS ux,
        CASE
            WHEN roadbed_side.right_side THEN -generic_midpoint_step.dx
            ELSE generic_midpoint_step.dx
        END AS uy
    FROM roadbed_attributes
    INNER JOIN roadbed_side ON roadbed_attributes.globalid = roadbed_side.globalid
    INNER JOIN generic_midpoint_step
        ON roadbed_attributes.generic_segmentid = generic_midpoint_step.generic_segmentid
),

crossings AS (
    SELECT
        globalid,
        mx,
        my,
        ux,
        uy,
        unnest(st_dump(st_intersection(
            geom,
            st_makeline(
                st_point(mx - 100000 * ux, my - 100000 * uy),
                st_point(mx + 100000 * ux, my + 100000 * uy)
            )
        ))).geom AS crossing
    FROM perpendiculars
),

crossing_offsets AS (
    SELECT
        globalid,
        (st_x(crossing) - mx) * ux + (st_y(crossing) - my) * uy AS t
    FROM crossings
    WHERE st_geometrytype(crossing) = 'POINT'
),

roadbed_distance AS (
    SELECT
        roadbed_side.globalid,
        roadbed_side.right_side,
        coalesce(
            min(crossing_offsets.t) FILTER (WHERE crossing_offsets.t >= 0.1),
            0.1 - max(crossing_offsets.t) FILTER (WHERE crossing_offsets.t < 0),
            0.1
        ) AS distance_to_generic
    FROM roadbed_side
    LEFT JOIN crossing_offsets ON roadbed_side.globalid = crossing_offsets.globalid
    GROUP BY roadbed_side.globalid, roadbed_side.right_side
),

-- RPLRecordOrderUpdate: each list sorted outermost first (ties keep the cursor's order,
-- OBJECTID within the generic), then its first R (right list) or L (left list) moved to
-- its head; the right list is written first.
prod_sorted AS (
    SELECT
        spec_order.globalid,
        roadbed_distance.right_side,
        roadbed_distance.distance_to_generic,
        spec_order.roadbed_position_code
        = CASE WHEN roadbed_distance.right_side THEN 'R' ELSE 'L' END AS heads_its_list,
        spec_order.generic_segmentid,
        spec_order.ogc_fid
    FROM spec_order
    INNER JOIN roadbed_distance ON spec_order.globalid = roadbed_distance.globalid
),

prod_order AS (
    SELECT
        globalid,
        right_side,
        distance_to_generic,
        row_number() OVER (
            PARTITION BY generic_segmentid
            ORDER BY
                NOT right_side ASC,
                -- only the list's first R or L moves
                CASE WHEN head_rank = 1 AND heads_its_list THEN 0 ELSE 1 END ASC,
                distance_to_generic DESC,
                ogc_fid ASC
        ) AS prod_group_order_id
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY generic_segmentid, right_side, heads_its_list
                ORDER BY distance_to_generic DESC, ogc_fid ASC
            ) AS head_rank
        FROM prod_sorted
    ) AS ranked
),

ordered AS (
    SELECT
        spec_order.*,
        prod_order.right_side,
        prod_order.distance_to_generic,
        prod_order.prod_group_order_id,
        {% if var('fix_rpl_roadbed_order') -%}
            spec_order.spec_group_order_id
        {%- else -%}
            prod_order.prod_group_order_id
        {%- endif %} AS group_order_id
    FROM spec_order
    INNER JOIN prod_order ON spec_order.globalid = prod_order.globalid
),

final AS (
    SELECT
        generic_segmentid || '_' || group_order_id AS rpl_id,
        globalid,
        generic_segmentid,
        generic_segmenttype,
        roadbed_segmentid,
        group_order_id,
        prod_group_order_id,
        spec_group_order_id,
        right_side,
        distance_to_generic,
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
    FROM ordered
    ORDER BY rpl_id
)

SELECT * FROM final

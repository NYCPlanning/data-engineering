{{ config(materialized='table') }}

-- Node-to-street-name crosswalk for the published LION GDB (nyclion_*.zip).
-- Rewritten from the actual legacy source (Report_NY_Nodestr(12B).py, J. Ding/ITD-DCP) -
-- not the ETL spec, which has no coverage of this layer at all. The previous version of
-- this model built names from dcp_cscl_streetname's pre-formatted lookup_key plus a
-- statistically-derived abbreviation layer (see git history / old CSCL-LION-09 writeup);
-- that was working around the wrong source field. lookup_key is spelled-out
-- ("WEST 174 STREET"); prod's actual output is abbreviated ("W 174 ST") because the real
-- algorithm concatenates the already-abbreviated structured fields directly
-- (pre_directional="W", post_type="ST", ...), never touching lookup_key. Confirmed against
-- dcp_cscl_streetname directly: pre_directional/post_type store the short form, lookup_key
-- the long form.
--
-- Key points from the legacy script:
--   - Street name = pre_value + " " + STREET_NAME + " " + post_value.
--     pre_value  = concat_ws(' ', pre_modifier, pre_directional, pre_type)
--     post_value = concat_ws(' ', post_type, post_directional, post_modifier)
--     Order is asymmetric (pre: modifier, directional, type; post: type, directional,
--     modifier) - confirmed from the source, not a guess. The nested None-checks in the
--     original Python reduce to exactly this null-skipping concatenation in every branch.
--   - dcp_cscl_featurename's FEATURE_NAME overrides the StreetName-built name whenever
--     both exist for the same B7SC (checked second in the legacy script, unconditionally
--     overwrites).
--   - If a segment's preferred B7SC has no match in either table, fall back to a
--     segmentid-keyed label: Subway.SUBWAY_LABEL, Rail.RAIL_LABEL,
--     Shoreline.SHORELINE_LABEL, NonStreetFeature.LINETYPE (mapped through a fixed 1-7
--     code table), Centerline.STNAME_LABEL - in that priority order.
--   - Node-to-segment comes from STREETSHAVEINTERSECTIONS, not spatial adjacency.
-- No leading space: an earlier version of this comment claimed prod prefixes every STNAME
-- with one (inferred from reading the legacy Create_table_from_txtfile script, not from
-- comparing real output), and that space survived this rewrite unquestioned. Checked
-- directly against prod's real, freshly-downloaded 26c node_stname layer
-- (production_outputs.fgdb_node_stname, loaded via
-- poc_validation/prod_data_loader.py's load_production_lion_fgdb_layers): zero of its
-- 245,529 rows have a leading space, and stripping ours makes every single dev row match a
-- prod row exactly (245,529/245,529, both directions). Whether the space assumption was
-- ever correct for an older release or the original verification compared against stale
-- production_outputs data is unknown - either way it's demonstrably wrong for 26c.
WITH principal_b7sc AS (
    SELECT
        segmentid,
        b7sc
    FROM {{ ref('int__lgc') }}
    WHERE lgc_rank = 1
),

street_names AS (
    SELECT
        b7sc,
        concat_ws(
            ' ',
            nullif(concat_ws(' ', pre_modifier, pre_directional, pre_type), ''),
            street_name,
            nullif(concat_ws(' ', post_type, post_directional, post_modifier), '')
        ) AS stname
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE principal_flag = 'Y'
),

feature_names AS (
    SELECT
        b7sc,
        feature_name AS stname
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
    WHERE principal_flag = 'Y'
),

-- FeatureName wins over StreetName when both exist for the same B7SC, matching the
-- legacy script's fn_dir override.
names_by_b7sc AS (
    SELECT
        coalesce(feature_names.b7sc, street_names.b7sc) AS b7sc,
        coalesce(feature_names.stname, street_names.stname) AS stname
    FROM street_names
    FULL JOIN feature_names ON street_names.b7sc = feature_names.b7sc
),

-- Fallback labels, keyed by segmentid, for segments whose preferred B7SC has no match
-- in either name table above.
fallback_names AS (
    SELECT
        segmentid,
        subway_label AS stname
    FROM {{ source('recipe_sources', 'dcp_cscl_subway') }}
    UNION ALL
    SELECT
        segmentid,
        rail_label
    FROM {{ source('recipe_sources', 'dcp_cscl_rail') }}
    UNION ALL
    SELECT
        segmentid,
        shoreline_label
    FROM {{ source('recipe_sources', 'dcp_cscl_shoreline') }}
    UNION ALL
    SELECT
        segmentid,
        CASE linetype
            WHEN 1 THEN 'ELECTION DISTRICT BOUNDARY'
            WHEN 2 THEN 'SCHOOL DISTRICT BOUNDARY'
            WHEN 3 THEN 'CENSUS BLOCK BOUNDARY'
            WHEN 4 THEN 'PIER OUTLINE'
            WHEN 5 THEN 'PHYSICAL NON STREET FEATURE'
            WHEN 6 THEN 'DISTRICT BOUNDARY'
            WHEN 7 THEN 'OTHER BOUNDARY'
        END AS stname
    FROM {{ source('recipe_sources', 'dcp_cscl_nonstreetfeatures') }}
    UNION ALL
    SELECT
        segmentid,
        stname_label
    FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
),

segment_names AS (
    SELECT
        principal_b7sc.segmentid,
        coalesce(names_by_b7sc.stname, fallback_names.stname) AS stname
    FROM principal_b7sc
    LEFT JOIN names_by_b7sc ON principal_b7sc.b7sc = names_by_b7sc.b7sc
    LEFT JOIN fallback_names ON principal_b7sc.segmentid = fallback_names.segmentid
)

SELECT DISTINCT
    streetshaveintersections.nodeid::int AS "NODEID",
    segment_names.stname AS "STNAME"
FROM {{ ref('stg__streetshaveintersections') }} AS streetshaveintersections
INNER JOIN segment_names ON streetshaveintersections.segmentid = segment_names.segmentid
WHERE segment_names.stname IS NOT NULL

-- for RPL records rows with diffs in their ordering, get and compare their ordering and along with relevent geometries
-- rpl_id indicates the order of roadbed_segmentid rows within each generic_segmentid grouping
WITH

diff_ids AS (
    SELECT DISTINCT generic_segmentid
    FROM
        {{ ref('qa__rpl_order') }}
    WHERE
        dbt_audit_row_status = 'modified'
),

focus_rpl_prod AS (
    SELECT
        prod_rpl.generic_segmentid || '_' || ROW_NUMBER() OVER (
            PARTITION BY prod_rpl.generic_segmentid
        ) AS rpl_id,
        prod_rpl.*
    FROM
        production_outputs.rpl AS prod_rpl
    INNER JOIN diff_ids
        ON
            prod_rpl.generic_segmentid = diff_ids.generic_segmentid
),
focus_rpl AS (
    SELECT dev_rpl.*
    FROM
        {{ ref('int__rpl') }} AS dev_rpl
    INNER JOIN diff_ids
        ON
            LPAD(dev_rpl.generic_segmentid::TEXT, 7, '0') = diff_ids.generic_segmentid
),
focus_lion AS (
    SELECT dev_lion.*
    FROM
        {{ ref('int__lion') }} AS dev_lion
    INNER JOIN diff_ids
        ON
            LPAD(dev_lion.segmentid::TEXT, 7, '0') = diff_ids.generic_segmentid
),

all_segments AS (
    SELECT
        focus_rpl.generic_segmentid,
        focus_rpl.roadbed_segmentid,
        LPAD(focus_rpl.rpl_id::TEXT, 7, '0') AS rpl_id_dev,
        focus_rpl_prod.rpl_id AS rpl_id_prod,
        focus_rpl.roadbed_position_code,
        focus_rpl.generic_segmenttype,
        ST_TRANSFORM(focus_rpl.midpoint, 4326) AS midpoint,
        ST_TRANSFORM(focus_rpl.geom, 4326) AS geom,
        null AS midpoint_generic,
        null AS geom_generic
    FROM
        focus_rpl
    LEFT JOIN focus_rpl_prod
        ON
            LPAD(focus_rpl.generic_segmentid::TEXT, 7, '0') = focus_rpl_prod.generic_segmentid
            AND LPAD(focus_rpl.roadbed_segmentid::TEXT, 7, '0') = focus_rpl_prod.roadbed_segmentid
    UNION ALL
    SELECT
        segmentid AS generic_segmentid,
        null AS roadbed_segmentid,
        null AS rpl_id_dev,
        null AS rpl_id_prod,
        null AS roadbed_position_code,
        null AS generic_segmenttype,
        null AS midpoint,
        null AS geom,
        ST_TRANSFORM(midpoint, 4326) AS midpoint_generic,
        ST_TRANSFORM(geom, 4326) AS geom_generic
    FROM
        focus_lion
),

final_cte AS (
    SELECT *
    FROM
        all_segments
    ORDER BY
        generic_segmentid ASC,
        rpl_id_dev ASC
)

SELECT *
FROM
    final_cte

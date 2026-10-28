-- ETL spec 2.6.4 (L49): a proto-segment whose geometry-based segment has curve flag L or R
-- takes the opposite value when the proto is reversed (FROM_TO_INDICATOR = 'R'), and the
-- same value otherwise. Returns the protos that break that.

WITH protos AS (
    SELECT
        lion.globalid,
        lion.lionkey,
        lion.curve_flag,
        coalesce(proto.reversed, FALSE) AS reversed,
        parent.curve_flag AS parent_curve_flag
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('stg__altsegmentdata_proto') }} AS proto ON lion.globalid = proto.globalid
    INNER JOIN {{ ref('int__centerline_curve') }} AS parent ON lion.segmentid = parent.segmentid
    WHERE
        lion.source_table = 'altsegmentdata'
        AND lion.feature_type = 'centerline'
        AND parent.curve_flag IN ('L', 'R')
)

SELECT *
FROM protos
WHERE
    curve_flag IS DISTINCT FROM CASE
        WHEN NOT reversed THEN parent_curve_flag
        WHEN parent_curve_flag = 'L' THEN 'R'
        ELSE 'L'
    END

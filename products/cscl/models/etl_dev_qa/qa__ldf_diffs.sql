/*
The individual LDF records that dev and prod disagree on, with their identifiers pulled
out so they can be chased without decoding fixed-width text by hand.

See qa__ldf_summary for why comparison is on positions 1-90 only. The transitory rule is a
port of prod's, so the only expected rows are the G and P records in CSCL-LDF-01; anything
else is a new difference.

CSCL-LDF-01 is matched by rule: a dev-only G or P record, other than a delete, whose old
or new ID is deleted anywhere in the journal, which is prod's IsSegmentdeleted. On 26c that
rule hits exactly the 12 dev-only records and no record both sides publish. Prod keeps one set of deleted IDs
for the whole run and processes S, then G, then P, so a G record sees S and G deletes and a
P record sees all three.
*/

WITH bodies AS (
    SELECT
        'dev' AS source,
        substring(dat_column, 1, 90) AS record_body
    FROM {{ ref('ldf_base') }}
    UNION ALL
    SELECT
        'prod' AS source,
        substring(dat_column, 1, 90) AS record_body
    FROM {{ source('production_outputs', 'ldf_base') }}
),

counted AS (
    SELECT
        record_body,
        count(*) FILTER (WHERE source = 'dev') AS dev_count,
        count(*) FILTER (WHERE source = 'prod') AS prod_count
    FROM bodies
    GROUP BY record_body
),

diffs AS (
    SELECT
        CASE WHEN dev_count > prod_count THEN 'dev_only' ELSE 'prod_only' END AS missing_from,
        abs(dev_count - prod_count) AS records,
        substring(record_body, 1, 1) AS record_type,
        substring(record_body, 3, 1) AS action_code,
        -- Node records: identifier and location. Blank on segment records.
        nullif(trim(substring(record_body, 32, 7)), '') AS nodeid,
        nullif(trim(substring(record_body, 11, 7)), '') AS x_coord_or_old_id,
        nullif(trim(substring(record_body, 18, 7)), '') AS y_coord,
        -- Segment records: the two IDs. old_id shares position 11-17 with the node X coord.
        nullif(trim(substring(record_body, 44, 7)), '') AS new_id,
        nullif(trim(substring(record_body, 28, 7)), '') AS old_from_nodeid,
        nullif(trim(substring(record_body, 35, 7)), '') AS old_to_nodeid,
        nullif(trim(substring(record_body, 61, 7)), '') AS new_from_nodeid,
        nullif(trim(substring(record_body, 68, 7)), '') AS new_to_nodeid,
        record_body
    FROM counted
    WHERE dev_count <> prod_count
),

journal_deletes AS (
    SELECT
        record_type,
        old_segment_id AS id
    FROM {{ ref('stg__centerlinehistory') }}
    WHERE
        release_num IS NULL
        AND upper(trim(action_code)) = 'D'
        AND coalesce(old_segment_id, 0) <> 0
    UNION
    SELECT
        record_type,
        new_segment_id AS id
    FROM {{ ref('stg__centerlinehistory') }}
    WHERE
        release_num IS NULL
        AND upper(trim(action_code)) = 'D'
        AND coalesce(old_segment_id, 0) <> 0
        AND coalesce(new_segment_id, 0) <> 0
),

labeled AS (
    SELECT
        diffs.*,
        CASE
            WHEN
                diffs.missing_from = 'dev_only'
                AND diffs.record_type IN ('G', 'P')
                AND diffs.action_code <> 'D'
                AND EXISTS (
                    SELECT 1 FROM journal_deletes AS jd
                    WHERE
                        (jd.record_type IN ('S', 'G') OR jd.record_type = diffs.record_type)
                        AND jd.id IN (
                            try_cast(diffs.x_coord_or_old_id AS bigint),
                            try_cast(diffs.new_id AS bigint)
                        )
                )
                THEN 'CSCL-LDF-01: ID deleted elsewhere in the journal'
        END AS known_difference
    FROM diffs
)

SELECT
    *,
    known_difference IS NOT NULL AS accounted_for
FROM labeled
ORDER BY record_type, action_code, record_body

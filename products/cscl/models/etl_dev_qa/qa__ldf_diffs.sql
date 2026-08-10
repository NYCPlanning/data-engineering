/*
The individual LDF records that dev and prod disagree on, with their identifiers pulled
out so they can be chased without decoding fixed-width text by hand.

See qa__ldf_summary for why comparison is on positions 1-90 only. Most of what shows up
here should be transitory-elimination differences - see design_doc.md, "Known dev/prod
differences" - so check that before treating a row as a new defect.
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
)

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
ORDER BY record_type, action_code, record_body

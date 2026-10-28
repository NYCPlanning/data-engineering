{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

/*
Dev/prod agreement for the LDF header record, on positions 1-90 only - matching
qa__ldf_summary's treatment of the base LDF. Positions 91-100 hold the header's own
cumulative record number, mechanical and known-noisy (see int__ldf_header.sql,
CSCL-LDF-03), so it's excluded the same way.

There's exactly one header record per side, so this is a single-row result rather
than a breakdown by record type/action code.

The record count (positions 40-45) follows the base LDF, so a header that differs only
there, by exactly the net of the CSCL-LDF-01 records in qa__ldf_diffs, is accounted for
by CSCL-LDF-01.
*/

WITH bodies AS (
    SELECT
        'dev' AS source,
        substring(dat_column, 1, 90) AS record_body
    FROM {{ ref('ldf_header') }}
    UNION ALL
    SELECT
        'prod' AS source,
        substring(dat_column, 1, 90) AS record_body
    FROM {{ source('production_outputs', 'ldf_header') }}
),

counted AS (
    SELECT
        record_body,
        count(*) FILTER (WHERE source = 'dev') AS dev_count,
        count(*) FILTER (WHERE source = 'prod') AS prod_count
    FROM bodies
    GROUP BY record_body
),

record_counts AS (
    SELECT
        max(substring(record_body, 40, 6)::int) FILTER (WHERE source = 'dev')
        - max(substring(record_body, 40, 6)::int) FILTER (WHERE source = 'prod')
            AS record_count_delta,
        count(DISTINCT substring(record_body, 1, 39) || substring(record_body, 46))
        = 1 AS same_except_record_count
    FROM bodies
),

ldf_01_net AS (
    SELECT
        coalesce(sum(
            CASE WHEN missing_from = 'dev_only' THEN records ELSE -records END
        ), 0) AS records
    FROM {{ ref('qa__ldf_diffs') }}
    WHERE accounted_for
),

summary AS (
    SELECT
        sum(dev_count) AS dev_records,
        sum(prod_count) AS prod_records,
        sum(least(dev_count, prod_count)) AS matched,
        sum(greatest(dev_count - prod_count, 0)) AS dev_only,
        sum(greatest(prod_count - dev_count, 0)) AS prod_only
    FROM counted
),

accounted AS (
    SELECT
        summary.*,
        summary.matched = 0 AS differs,
        record_counts.record_count_delta,
        coalesce(
            summary.matched = 0
            AND record_counts.same_except_record_count
            AND record_counts.record_count_delta = ldf_01_net.records,
            FALSE
        ) AS accounted_for
    FROM summary
    CROSS JOIN record_counts
    CROSS JOIN ldf_01_net
)

SELECT
    *,
    CASE
        WHEN accounted_for THEN 'CSCL-LDF-01: record count follows the CSCL-LDF-01 records'
    END AS known_difference
FROM accounted

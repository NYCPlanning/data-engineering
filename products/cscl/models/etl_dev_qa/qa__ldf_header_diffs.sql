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
)

SELECT
    sum(dev_count) AS dev_records,
    sum(prod_count) AS prod_records,
    sum(least(dev_count, prod_count)) AS matched,
    sum(greatest(dev_count - prod_count, 0)) AS dev_only,
    sum(greatest(prod_count - dev_count, 0)) AS prod_only
FROM counted

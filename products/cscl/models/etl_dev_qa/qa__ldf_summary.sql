/*
Dev/prod agreement for the LDF, by record type and action code.

Records are compared on positions 1-90 only. Positions 91-100 hold the cumulative record
number, which is assigned from each file's own ordering - so if the two files differ by a
single record, every subsequent number shifts and a naive whole-line comparison would
report that every record differs. Comparing the record body isolates real disagreements.

Counts, not sets: the LDF legitimately contains duplicate records (the same change posted
twice against one segment), so multiplicity is preserved on both sides.
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
    substring(record_body, 1, 1) AS record_type,
    substring(record_body, 3, 1) AS action_code,
    sum(dev_count) AS dev_records,
    sum(prod_count) AS prod_records,
    sum(least(dev_count, prod_count)) AS matched,
    sum(greatest(dev_count - prod_count, 0)) AS dev_only,
    sum(greatest(prod_count - dev_count, 0)) AS prod_only
FROM counted
GROUP BY substring(record_body, 1, 1), substring(record_body, 3, 1)
ORDER BY substring(record_body, 1, 1), substring(record_body, 3, 1)

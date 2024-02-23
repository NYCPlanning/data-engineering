-- create _kpdb table
DROP TABLE IF EXISTS _kpdb_combined_and_deduped;
DROP TABLE IF EXISTS _kpdb;

SELECT
    a.*,
    b.project_id,
    COALESCE(b.units_net, a.units_gross) AS units_net,
    ROUND(
        COALESCE(a.prop_within_5_years::decimal, 0)
        * COALESCE(b.units_net, a.units_gross)::decimal
    ) AS within_5_years,
    ROUND(
        COALESCE(a.prop_5_to_10_years::decimal, 0) * b.units_net::decimal
    ) AS from_5_to_10_years,
    ROUND(
        COALESCE(a.prop_after_10_years::decimal, 0) * b.units_net::decimal
    ) AS after_10_years,
    COALESCE(
        NULLIF(prop_within_5_years, 0), NULLIF(prop_5_to_10_years, 0), NULLIF(prop_after_10_years, 0)
    ) IS NOT NULL
    AS has_project_phasing
INTO _kpdb_combined_and_deduped
FROM combined AS a
LEFT JOIN deduped_units AS b
    ON a.record_id = b.record_id
WHERE a.no_classa = '0' OR a.no_classa IS NULL;

-- fix +/-1 rounding error in calculation of units by year ranges
--  for all rows with a rounding error
WITH
net_units_details AS (
    SELECT
        *,
        NULLIF(units_net, 0) IS NOT NULL AS has_future_units,
        within_5_years + from_5_to_10_years + after_10_years AS net_units_sum,
        units_net - (within_5_years + from_5_to_10_years + after_10_years) AS net_units_diff
    FROM _kpdb_combined_and_deduped
),

future_units_details AS (
    SELECT
        *,
        CASE
            WHEN
                has_future_units AND NOT has_project_phasing
                THEN units_net
            ELSE 0
        END AS future_units_without_phasing
    FROM net_units_details
)

SELECT *
INTO _kpdb
FROM future_units_details;

--  add the net unit difference (the rounding error) to the first phase with units
UPDATE _kpdb SET
    within_5_years = within_5_years + net_units_diff,
    net_units_diff = 0
WHERE has_future_units AND net_units_diff != 0 AND NULLIF(prop_within_5_years, 0) IS NOT NULL;

UPDATE _kpdb SET
    from_5_to_10_years = from_5_to_10_years + net_units_diff,
    net_units_diff = 0
WHERE has_future_units AND net_units_diff != 0 AND NULLIF(prop_5_to_10_years, 0) IS NOT NULL;

UPDATE _kpdb SET
    after_10_years = after_10_years + net_units_diff,
    net_units_diff = 0
WHERE has_future_units AND net_units_diff != 0 AND NULLIF(prop_after_10_years, 0) IS NOT NULL;

-- determine missing borough values
UPDATE _kpdb a
SET
    borough
    = CASE
        WHEN a.borough IS NOT NULL THEN a.borough
        WHEN
            a.source = 'DCP Application' AND a.record_id LIKE 'P%'
            THEN
                (
                    '{"M": 1, "X": 2, "K": 3, "Q": 4, "R": 5}'::json
                    -> SUBSTRING(a.record_id, 6, 1)
                )::varchar
        WHEN
            a.source = 'DCP Application'
            THEN
                (
                    '{"M": 1, "X": 2, "K": 3, "Q": 4, "R": 5}'::json
                    -> SUBSTRING(a.record_id, 5, 1)
                )::varchar
        WHEN a.source = 'DOB' THEN SUBSTRING(a.record_id, 1, 1)
    END
WHERE borough IS NULL;

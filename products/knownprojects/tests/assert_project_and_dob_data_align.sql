{{ config(severity='warn') }}

-- Warn when the ZAP and DOB pipelines describe different periods.
--
-- KPDB's forward-looking columns assume every source covers the same window.
-- They don't error when one lags: a ZAP pin 18 months behind simply means no
-- DCP Application filed since then is present, so the pipeline is understated
-- with no other symptom. Boundary layers are excluded — their age doesn't
-- affect which projects exist, only where they land.
--
-- Both sources record dates as YYYY/MM/DD, verified across all rows.

WITH newest AS (
    SELECT
        max(to_date(date, 'YYYY/MM/DD')) FILTER (
            WHERE source = 'DCP Application'
        ) AS newest_dcp_application,
        max(to_date(date, 'YYYY/MM/DD')) FILTER (
            WHERE source = 'DOB'
        ) AS newest_dob
    FROM {{ ref('kpdb') }}
    WHERE source IN ('DCP Application', 'DOB') AND nullif(date, '') IS NOT NULL
)

SELECT
    newest_dcp_application,
    newest_dob,
    abs(newest_dob - newest_dcp_application) AS days_apart
FROM newest
WHERE abs(newest_dob - newest_dcp_application) > 183

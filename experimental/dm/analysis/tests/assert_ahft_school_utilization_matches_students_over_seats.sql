{{ config(severity='warn') }}
-- utilization as printed against students over seats, to catch a typo in either. Some are printed rounded to 0.001.
SELECT *
FROM {{ ref('stg_ahft__school_subdistricts') }}
WHERE abs(ps_students / ps_seats - ps_utilization) > 0.001
    OR abs(is_students / is_seats - is_utilization) > 0.001

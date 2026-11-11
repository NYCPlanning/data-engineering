-- FacDB's schools from DOE's LCGMS list. Leaves out the two FacDB couldn't place.
SELECT
    facility_id AS school_id,
    facility_name AS school_name,
    facility_type AS school_type,
    facility_subgroup AS school_subgroup,
    address,
    bbl,
    geom
FROM {{ ref('stg_dcp__facilities') }}
WHERE datasource = 'doe_lcgms' AND geom IS NOT NULL

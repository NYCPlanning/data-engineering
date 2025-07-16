/*
DESCRIPTION:
	1. Filters dcp_housing records to non-demolitions, non-withdrawn,
		non-zero net unit change, and positive proposed units. Only alterations
		with positive net unit chage are included.
	2. Maps fields from the dcp_housing schema to the KPDB schema.
	3. Uses record BBL to get lot-level polygon geometry from mappluto
INPUTS:
	dcp_housing
	dcp_mappluto_wi
OUTPUTS:
	dcp_housing_poly
*/

DROP TABLE IF EXISTS dcp_housing_poly;
WITH
/* Prior to geom steps, filter to relevant DOB jobs:
	- Excludes demolitions and withdrawn jobs.
	- For class A, exludes jobs with no residential unit change,
alterations removing units, and jobs where the proposed
residential units are 0.
	- For class B, and record with positive initial or proposed
units are included. These records only provide context for DOB
match review, and get excluded from the final KPDB output.
*/
dcp_housing_filtered AS (
    SELECT *
    FROM dcp_housing
    WHERE
        job_type != 'Demolition'
        AND job_status != '9. Withdrawn'
        AND (
            (
                classa_net::integer != 0
                AND classa_prop::integer > 0
                AND NOT (
                    job_type = 'Alteration'
                    AND classa_net::integer <= 0
                )
            )
            OR
            (
                otherb_prop::integer > 0
                OR otherb_init::integer > 0
            )
        )
),

--Join with mappluto on BBL to get polygon geom. 
bbl_join AS (
    SELECT
        a.job_number,
        a.bbl,
        a.wkb_geometry AS point_geom,
        b.wkb_geometry AS bbl_join_geom
    FROM dcp_housing_filtered AS a
    LEFT JOIN dcp_mappluto_wi AS b
        ON a.bbl = b.bbl::bigint::text
),

/* Spatial join with mappluto to get polygon geom where bbl geom failed
This happens as a separate step to limit the number of records needing
a spatial join. */
spatial_join AS (
    SELECT
        a.job_number,
        a.bbl,
        a.point_geom,
        b.wkb_geometry AS spatial_join_geom
    FROM bbl_join AS a
    INNER JOIN dcp_mappluto_wi AS b
        ON ST_Intersects(a.point_geom, b.wkb_geometry)
    WHERE a.bbl_join_geom IS NULL AND a.point_geom IS NOT NULL
),

-- Combine into a single geom lookup
_geom AS (
    SELECT
        a.job_number,
        a.bbl,
        coalesce(a.bbl_join_geom, b.spatial_join_geom) AS geom,
        (CASE
            WHEN a.bbl_join_geom IS NULL THEN 'Spatial'
            ELSE 'BBL'
        END) AS geom_source
    FROM bbl_join AS a
    LEFT JOIN spatial_join AS b
        ON a.job_number = b.job_number
)

SELECT
    'DOB' AS source,
    a.job_number::text AS record_id,
    NULL AS record_id_input,
    a.address AS record_name,
    a.job_desc,
    a.job_type AS type,
    a.classa_net AS units_gross,
    a.classa_init,
    a.classa_prop,
    a.otherb_init,
    a.otherb_prop,
    NULL::numeric AS prop_5_to_10_years,
    NULL::numeric AS prop_after_10_years,
    0 AS phasing_known,
    b.geom,
    b.geom_source,
    'DOB ' || a.job_status AS status,
    coalesce(
        to_char(to_date(a.date_permittd, 'YYYY-MM-DD'), 'YYYY/MM/DD'),
        to_char(to_date(a.date_filed, 'YYYY-MM-DD'), 'YYYY/MM/DD')
    ) AS date,
    (CASE
        WHEN a.date_permittd IS NOT NULL THEN 'Date Permitted'
        WHEN a.date_filed IS NOT NULL THEN 'Date Filed'
    END) AS date_type,

    -- Phasing
    to_char(
        to_date(a.date_permittd, 'YYYY-MM-DD'), 'YYYY/MM/DD'
    ) AS date_permittd,
    to_char(to_date(a.date_filed, 'YYYY-MM-DD'), 'YYYY/MM/DD') AS date_filed, -- remove phasing assumption with inactive dob 
    to_char(
        to_date(a.date_lastupdt, 'YYYY-MM-DD'), 'YYYY/MM/DD'
    ) AS date_lastupdt,
    to_char(
        to_date(a.date_complete, 'YYYY-MM-DD'), 'YYYY/MM/DD'
    ) AS date_complete,
    (CASE WHEN a.job_inactive ~* 'Inactive' THEN 1 ELSE 0 END) AS inactive,
    (CASE
        WHEN
            a.job_status
            ~* '1. Filed Application|2. Approved Application|3. Permitted for Construction'
            AND a.job_inactive IS NULL THEN 1
    END) AS prop_within_5_years,
    flag_nycha(a::text) AS nycha,
    (CASE
        WHEN a.otherb_init::integer > 0 OR a.otherb_prop::integer > 0 THEN '1'
        ELSE 0
    END) AS classb,
    (CASE
        WHEN
            a.classa_net::integer != 0
            AND a.classa_prop::integer > 0
            AND NOT (
                a.job_type = 'Alteration'
                AND a.classa_net::integer <= 0
            ) THEN '0'
        ELSE '1'
    END) AS no_classa,
    flag_senior_housing(a::text) AS senior_housing
INTO dcp_housing_poly
FROM dcp_housing_filtered AS a
INNER JOIN _geom AS b
    ON a.job_number = b.job_number;

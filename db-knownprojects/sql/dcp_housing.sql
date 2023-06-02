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
    FROM dcp_housing a
    WHERE a.job_type <> 'Demolition'
    AND a.job_status <> '9. Withdrawn'
	AND (
			(
				a.classa_net::integer <> 0
    			AND a.classa_prop::integer > 0
    			AND NOT (a.job_type = 'Alteration'
        		AND a.classa_net::integer <= 0)
			)
		OR 
			(
				a.otherb_prop::integer > 0
				OR a.otherb_init::integer > 0
			)
		)
),
--Join with mappluto on BBL to get polygon geom. 
bbl_join AS (
	SELECT 
		a.job_number,
		a.bbl,
		a.wkb_geometry as point_geom,
		b.wkb_geometry as bbl_join_geom
	FROM dcp_housing_filtered a
	LEFT JOIN dcp_mappluto_wi b
    ON a.bbl = b.bbl::bigint::text
),
/* Spatial join with mappluto to get polygon geom where bbl geom failed
This happens as a separate step to limit the number of records needing
a spatial join. */
spatial_join AS(
	SELECT 
		a.job_number,
		a.bbl,
		a.point_geom,
		b.wkb_geometry as spatial_join_geom
	FROM bbl_join a
	JOIN dcp_mappluto_wi b
	ON ST_Intersects(a.point_geom, b.wkb_geometry)
	WHERE a.bbl_join_geom IS NULL AND a.point_geom IS NOT NULL
),
-- Combine into a single geom lookup
_geom AS (
	SELECT a.job_number,
		a.bbl,
		COALESCE(a.bbl_join_geom, b.spatial_join_geom) as geom,
		(CASE
			WHEN a.bbl_join_geom IS NULL THEN 'Spatial'
			ELSE 'BBL'
		END) as geom_source
	FROM bbl_join a
	LEFT JOIN spatial_join b
	ON a.job_number = b.job_number
)

SELECT 
	'DOB' as source,
	a.job_number::text as record_id,
	NULL as record_id_input,
	a.address as record_name,
	a.job_desc,
	'DOB '||a.job_status as status,
	a.job_type as type,
	a.classa_net as units_gross,
	a.classa_init,
	a.classa_prop,
	a.otherb_init,
	a.otherb_prop,
	COALESCE(TO_CHAR(TO_DATE(a.date_permittd, 'YYYY-MM-DD'), 'YYYY/MM/DD'),
		TO_CHAR(TO_DATE(a.date_filed, 'YYYY-MM-DD'), 'YYYY/MM/DD')) as date,
	(CASE
		WHEN a.date_permittd IS NOT NULL THEN 'Date Permitted'
		WHEN a.date_filed IS NOT NULL THEN 'Date Filed'
	END) as date_type,
	TO_CHAR(TO_DATE(a.date_permittd, 'YYYY-MM-DD'), 'YYYY/MM/DD') as date_permittd,
	TO_CHAR(TO_DATE(a.date_filed, 'YYYY-MM-DD'), 'YYYY/MM/DD') as date_filed,
	TO_CHAR(TO_DATE(a.date_lastupdt, 'YYYY-MM-DD'), 'YYYY/MM/DD') as date_lastupdt,
	TO_CHAR(TO_DATE(a.date_complete, 'YYYY-MM-DD'), 'YYYY/MM/DD') as date_complete,
	(CASE WHEN a.job_inactive ~* 'Inactive' THEN 1 ELSE 0 END) as inactive,

    -- Phasing
    (CASE 
        WHEN a.job_status ~* '1. Filed Application|2. Approved Application|3. Permitted for Construction' 
            AND a.job_inactive IS NULL THEN 1 
        ELSE NULL
    END) as prop_within_5_years,
    NULL::numeric as prop_5_to_10_years, -- remove phasing assumption with inactive dob 
    NULL::numeric as prop_after_10_years,
    0 as phasing_known,
    flag_nycha(a::text) as nycha,
	(CASE
		WHEN a.otherb_init::integer > 0 OR a.otherb_prop::integer > 0 THEN '1' 
		ELSE 0
	END) as classb,
	(CASE
		WHEN a.classa_net::integer <> 0
    		AND a.classa_prop::integer > 0
    		AND NOT (a.job_type = 'Alteration'
        		AND a.classa_net::integer <= 0) THEN '0'
		ELSE '1'
	END) as no_classa,
    flag_senior_housing(a::text) as senior_housing,
	b.geom,
	b.geom_source
INTO dcp_housing_poly
FROM dcp_housing_filtered a
JOIN _geom b
ON a.job_number = b.job_number;
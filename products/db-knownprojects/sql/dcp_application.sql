/*
DESCRIPTION:
	

INPUTS:
	dcp_projects
	dcp_projectactions
    	dcp_project_bbls
    	dcp_mappluto_wi
    	dcp_knownprojects
    	corrections_main
		zap_record_ids
OUTPUTS: 
	dcp_application
*/
DROP TABLE IF EXISTS _dcp_application;
WITH 
zap_translated as (
	SELECT
	    dcp_name,
	    dcp_projectname,
	    dcp_projectbrief,
	    dcp_projectdescription,
	    (CASE 
	    	WHEN dcp_applicanttype::numeric = 717170000 THEN 'DCP'
	    	WHEN dcp_applicanttype::numeric = 717170001 THEN 'Other Public Agency'
	    	WHEN dcp_applicanttype::numeric = 717170002 THEN 'Private'
	    END) as dcp_applicanttype,
	    (CASE
	    	WHEN dcp_visibility::numeric = 717170002 THEN 'Applicant Only'
	    	WHEN dcp_visibility::numeric = 717170001 THEN 'CPC Only'
	    	WHEN dcp_visibility::numeric = 717170003 THEN 'General Public'
	    	WHEN dcp_visibility::numeric = 717170000 THEN 'Internal DCP Only'
	    	WHEN dcp_visibility::numeric = 717170004 THEN 'LUP'
	    END) as dcp_visibility,
	    (CASE 
	    	WHEN dcp_borough::numeric = 717170000 then 'Bronx'
	    	WHEN dcp_borough::numeric = 717170002 then 'Brooklyn'
	    	WHEN dcp_borough::numeric = 717170001 then 'Manhattan'
	    	WHEN dcp_borough::numeric = 717170004 then 'Staten Island'
	    	WHEN dcp_borough::numeric = 717170005 then 'Citywide'
	    	WHEN dcp_borough::numeric = 717170003 then 'Queens'
	    END) as dcp_borough,
	    (CASE 
	    	WHEN statuscode::numeric = 1 THEN 'Active'
	    	WHEN statuscode::numeric = 717170000 THEN 'On-Hold'
	    	WHEN statuscode::numeric = 707070003 THEN 'Record Closed'
	    	WHEN statuscode::numeric = 707070000 THEN 'Complete'
	    	WHEN statuscode::numeric = 707070002 THEN 'Terminated'
	    	WHEN statuscode::numeric = 707070001 THEN 'Withdrawn-Applicant Unresponsive'
	    	WHEN statuscode::numeric = 717170001 THEN 'Withdrawn-Other'
	    END) as statuscode,
	    (CASE 
	    	WHEN dcp_publicstatus::numeric = 717170000 THEN 'Filed'
	    	WHEN dcp_publicstatus::numeric = 717170001 THEN 'In Public Review'
	    	WHEN dcp_publicstatus::numeric = 717170002 THEN 'Completed'
	    	WHEN dcp_publicstatus::numeric = 717170005 THEN 'Noticed'
	    END) as dcp_publicstatus,
        (CASE 
            WHEN dcp_projectphase::numeric = 717170000 THEN 'Study'
	    	WHEN dcp_projectphase::numeric = 717170001 THEN 'Pre-Pas'
	    	WHEN dcp_projectphase::numeric = 717170002 THEN 'Pre-Cert'
            WHEN dcp_projectphase::numeric = 717170003 THEN 'Public Review'
            WHEN dcp_projectphase::numeric = 717170004 THEN 'Public Completed'
	    	WHEN dcp_projectphase::numeric = 717170005 THEN 'Initiation'
        END) as dcp_projectphase,
	    (CASE WHEN dcp_projectcompleted IS NULL THEN NULL
	        ELSE TO_CHAR(dcp_projectcompleted::timestamp, 'YYYY/MM/DD') 
	    END)  as dcp_projectcompleted,
		(CASE WHEN coalesce(dcp_certifiedreferred, dcp_dcptargetcertificationdate) IS NULL THEN NULL
			ELSE TO_CHAR(coalesce(dcp_certifiedreferred, dcp_dcptargetcertificationdate)::timestamp, 'YYYY/MM/DD') 
		END) as dcp_certifiedreferred,
	    COALESCE(dcp_totalnoofdusinprojecd::numeric, 0) as dcp_totalnoofdusinprojecd,
	    COALESCE(dcp_mihdushighernumber::numeric, 0) as dcp_mihdushighernumber,
	    COALESCE(dcp_mihduslowernumber::numeric, 0) as dcp_mihduslowernumber,
	    COALESCE(dcp_numberofnewdwellingunits::numeric, 0) as dcp_numberofnewdwellingunits,
	    COALESCE(dcp_noofvoluntaryaffordabledus::numeric, 0) as dcp_noofvoluntaryaffordabledus,
	    COALESCE(dcp_residentialsqft::numeric, 0) as dcp_residentialsqft
	FROM dcp_projects
),
-- we exclude projects that have record closed, terminated or withdrawn as status
status_filter as (
    SELECT dcp_name
    FROM zap_translated
    WHERE statuscode !~* 'Record Closed|Terminated|Withdrawn'
),
-- we include projects have any clear indication of residential components
resid_units_filter as (
    SELECT dcp_name
    FROM zap_translated
    WHERE dcp_residentialsqft > 0
    or dcp_totalnoofdusinprojecd > 0
    or dcp_mihdushighernumber > 0
    or dcp_mihduslowernumber > 0
    or dcp_numberofnewdwellingunits > 0
    or dcp_noofvoluntaryaffordabledus >0
),
--all projects that are after 2010, and NULLs included
year_filter as (
    SELECT dcp_name
    FROM zap_translated
    WHERE (extract(year FROM dcp_projectcompleted::date) >= 2010
    or extract(year FROM dcp_certifiedreferred::date) >= 2010
    or (dcp_projectcompleted is NULL and dcp_certifiedreferred is NULL))
),
records_last_kpdb as (
	SELECT record_id as dcp_name
	FROM dcp_knownprojects
	WHERE source = 'DCP Application'
),
records_corr_add as (
	-- SELECT record_id as dcp_name
	-- FROM corrections_zap
	-- WHERE lower(action) = 'add'

	-- Everything in zap_record_ids -> add
	SELECT record_id as dcp_name
	FROM zap_record_ids
),
records_corr_remove as (
	-- SELECT record_id as dcp_name
	-- FROM corrections_zap
	-- WHERE lower(action) = 'remove'

	-- Everything not in zap_record_ids -> remove
	SELECT dcp_name FROM zap_translated
	WHERE dcp_name NOT IN (
		SELECT dcp_name FROM records_corr_add
	)
),
consolidated_add_filter as (
	/*
	Add if a record satisfies:
	1) flagged as add in corrections_zap.csv
	2) or ( flag_year = 1 and flag_status = 1 ) 
	*/
    SELECT distinct dcp_name FROM zap_translated a
    WHERE dcp_name IN (SELECT dcp_name FROM status_filter)
    AND dcp_name IN (SELECT dcp_name FROM year_filter)
    UNION SELECT dcp_name FROM records_corr_add 
),
consolidated_remove_filter as (
	/*
	Remove if a record satisfies:
	1) flagged as remove in corrections_zap.csv
	2) or not in consolidated_add_filter
	*/
	SELECT dcp_name FROM records_corr_remove UNION
    SELECT dcp_name as dcp_name FROM zap_translated
    WHERE dcp_name NOT IN (SELECT dcp_name FROM consolidated_add_filter)
),
relevant_projects as (
    SELECT DISTINCT dcp_name
    FROM zap_translated
    WHERE dcp_name IN (SELECT dcp_name FROM consolidated_add_filter)
	AND dcp_name NOT IN (SELECT dcp_name FROM consolidated_remove_filter)
)
SELECT distinct 
--descriptor fields
'DCP Application' as source,
(CASE WHEN dcp_name in (
	select distinct dcp_name 
	from relevant_projects) then 1
	else 0 end) as flag_relevant,

(CASE WHEN dcp_name in (
	select distinct dcp_name 
	from year_filter) then 1
	else 0 end) as flag_year,

(CASE WHEN dcp_name in (
	select distinct dcp_name 
	from status_filter) then 1
	else 0 end) as flag_status,

(CASE WHEN dcp_name in (
	select distinct dcp_name 
	from resid_units_filter) then 1
	else 0 end) as flag_resid_units,

(CASE WHEN dcp_name in (
	select dcp_name from records_last_kpdb)
	THEN 1 ELSE 0 END) as flag_in_last_kpdb,

(CASE WHEN dcp_name not in (
	select dcp_name from records_last_kpdb) 
	THEN 1 ELSE 0 END) as flag_not_in_last_kpdb,

(CASE 
WHEN dcp_name in (select dcp_name from records_corr_remove) THEN 'remove' 
WHEN dcp_name in (select dcp_name from records_corr_add) THEN 'add' 
	END) as flag_corrected,

dcp_name as record_id,
dcp_projectname as record_name,
dcp_projectbrief, 
dcp_projectdescription,
dcp_borough as borough,
statuscode,
(case
when dcp_projectphase ~* 'project completed' then 'DCP 4: Zoning Implemented'
when dcp_projectphase ~* 'pre-pas|pre-cert' then 'DCP 2: Application in progress'
when dcp_projectphase ~* 'initiation' then 'DCP 1: Expression of interest'
when dcp_projectphase ~* 'public review' then 'DCP 3: Certified/Referred'
end) as status,
dcp_publicstatus as publicstatus,
dcp_certifiedreferred,
dcp_applicanttype as applicanttype,
dcp_visibility as visibility,

--units fields
dcp_numberofnewdwellingunits,
dcp_totalnoofdusinprojecd,
dcp_mihdushighernumber,
dcp_mihduslowernumber,
dcp_noofvoluntaryaffordabledus,
dcp_residentialsqft, 

--calculate units_gross
COALESCE(
	nullif(dcp_numberofnewdwellingunits,0),
	nullif(dcp_totalnoofdusinprojecd,0),
	nullif(dcp_mihdushighernumber+ 
		dcp_noofvoluntaryaffordabledus,0),
	nullif(dcp_mihduslowernumber+ 
		dcp_noofvoluntaryaffordabledus,0)
)::numeric as units_gross,

--identify unit source
COALESCE(
	(case when nullif(dcp_numberofnewdwellingunits,0) is not NULL 
		then 'dcp_numberofnewdwellingunits' end),
	(case when nullif(dcp_totalnoofdusinprojecd,0) is not NULL 
		then 'dcp_totalnoofdusinprojecd' end),
	(case when nullif(dcp_mihdushighernumber+dcp_noofvoluntaryaffordabledus,0) is not NULL 
		then 'dcp_mihdushighernumber + dcp_noofvoluntaryaffordabledus' end),
	(case when nullif(dcp_mihduslowernumber+ dcp_noofvoluntaryaffordabledus,0) is not NULL 
		then 'dcp_mihduslowernumber + dcp_noofvoluntaryaffordabledus' end)
	) 
AS units_gross_source
INTO _dcp_application
FROM zap_translated;

DROP TABLE IF EXISTS dcp_application;
WITH
-- Assigning Geometry Using BBL
geom_pluto as (
	SELECT
		a.record_id,
		ST_Union(b.wkb_geometry) as geom
	FROM(
		SELECT 
			a.record_id,
			b.dcp_bblnumber as bbl
		from _dcp_application a
		LEFT JOIN dcp_projectbbls b
		ON a.record_id = trim(split_part(b.dcp_name, '-', 1))
		WHERE b.statuscode != '2'
	) a LEFT JOIN dcp_mappluto_wi b
	ON a.bbl::numeric = b.bbl::numeric
	GROUP BY a.record_id
),
-- Assigning Geometry Using Previous version of KPDB
geom_kpdb as (
	SELECT 
		a.record_id,
		case 
                 when b.geometry::geometry is NULL 
		then a.geom
                 else b.geometry::geometry
                end as geom
	FROM geom_pluto a
	LEFT JOIN dcp_knownprojects b
	ON a.record_id = b.record_id
),
-- Assigning Geometry Using Zoning Map Amendments
geom_ulurp as (
SELECT 
	a.record_id,
        case
        when st_union(b.wkb_geometry) is  NULL
	then a.geom
        else null
        end as geom
FROM(
	select 
		a.record_id,
		a.geom,
		b.dcp_ulurpnumber
	FROM (
		select
			a.record_id,
			a.geom,
			b.dcp_projectid
		from geom_kpdb a
		LEFT JOIN dcp_projects b
		on a.record_id = b.dcp_name
	) a LEFT JOIN dcp_projectactions b
	ON a.dcp_projectid = b._dcp_project_value
) a LEFT JOIN dcp_zoningmapamendments b
ON a.dcp_ulurpnumber = b.ulurpno
GROUP BY a.record_id, a.geom
)
-- Main table with the geometry lookup
SELECT a.*, b.geom
INTO dcp_application
FROM _dcp_application a
LEFT JOIN geom_ulurp b 
ON a.record_id = b.record_id;


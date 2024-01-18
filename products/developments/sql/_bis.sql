/*
DESCRIPTION:
	Initial field mapping and prelimilary data cleaning for BIS job applications data

INPUTS: 
	dob_jobapplications

OUTPUTS:
	_INIT_BIS_devdb (
		job_number text,
		job_type text,
		job_desc text,
		_occ_initial text,
		_occ_proposed text,
		stories_init numeric,
		stories_prop text,
		zoningsft_init numeric,
		zoningsft_prop numeric,
		classa_init numeric,
		classa_prop numeric,
		_job_status text,
		date_lastupdt text,
		date_filed text,
		date_statusd text,
		date_statusp text,
		date_statusr text,
		date_statusx text,
		zoningdist1 text,
		zoningdist2 text,
		zoningdist3 text,
		specialdist1 text,
		specialdist2 text,
		landmark text,
		ownership text,
		owner_name text,
		owner_biznm text,
		owner_address text,
		owner_zipcode text,
		owner_phone text,
		height_init text,
		height_prop text,
		constructnsf text,
		enlargement text,
		enlargementsf text,
		costestimate text,
		loftboardcert text,
		edesignation text,
		curbcut text,
		tracthomes text,
		address_numbr text,
		address_street text,
		address text,
		bin text,
		bbl text,
		boro text,
		x_withdrawal text,
		existingzoningsqft text,
		proposedzoningsqft text,
		buildingclass text,
		otherdesc text
	)
*/


DROP TABLE IF EXISTS _INIT_BIS_devdb;
SELECT
	jobnumber::text as job_number,

    -- Job Type recoding
	(CASE 
		WHEN jobtype = 'A1' THEN 'Alteration'
		WHEN jobtype = 'DM' THEN 'Demolition'
		WHEN jobtype = 'NB' THEN 'New Building'
		WHEN jobtype = 'A2' THEN 'Alteration (A2)'
		ELSE jobtype
	END ) as job_type,

	(CASE WHEN jobdescription !~ '[a-zA-Z]'
	THEN NULL ELSE jobdescription END) as job_desc,

    -- removing '.' for existingoccupancy 
    -- and proposedoccupancy (3 records affected)
	replace(existingoccupancy, '.', '') as _occ_initial, 
    replace(proposedoccupancy, '.', '') as _occ_proposed,
	
    -- set 0 -> null for jobtype = A1 or DM
	(CASE WHEN jobtype ~* 'A1|DM' 
        THEN nullif(existingnumstories, '0')::numeric
		ELSE NULL
    END) as stories_init,

	-- set 0 -> null for jobtype = A1 or NB
	(CASE WHEN jobtype ~* 'A1|NB' 
        THEN nullif(proposednumstories, '0')::numeric
		ELSE NULL
    END) as stories_prop,

    -- set 0 -> null for jobtype = A1 or DM\
	(CASE WHEN jobtype ~* 'A1|DM' 
        THEN nullif(existingzoningsqft, '0')::numeric
		ELSE existingzoningsqft::numeric
    END) as zoningsft_init,

    -- set 0 -> null for jobtype = A1 or DM
	(CASE WHEN jobtype ~* 'A1|DM' 
        THEN nullif(proposedzoningsqft, '0')::numeric
		ELSE proposedzoningsqft::numeric 
    END) as zoningsft_prop,

    -- if existingdwellingunits is not a number then null
        (CASE WHEN jobtype ~* 'NB' THEN 0 
        ELSE (CASE WHEN existingdwellingunits ~ '[^0-9]' THEN NULL
            ELSE existingdwellingunits::numeric END)
    END) as classa_init,

    -- if proposeddwellingunits is not a number then null
	(CASE WHEN jobtype ~* 'DM' THEN 0
		ELSE (CASE WHEN proposeddwellingunits ~ '[^0-9]' THEN NULL
			ELSE proposeddwellingunits::numeric END)
	END) as classa_prop,

	-- one to one mappings
	jobstatusdesc as _job_status,
	latestactiondate as date_lastupdt,
	prefilingdate as date_filed,
	fullypaid as date_statusd,
	approved as date_statusp,
	fullypermitted as date_statusr,
	signoffdate as date_statusx,
	zoningdist1 as ZoningDist1,
	zoningdist2 as ZoningDist2,
	zoningdist3 as ZoningDist3,
	specialdistrict1 as SpecialDist1,
	specialdistrict2 as SpecialDist2,

	(CASE WHEN landmarked = 'Y' THEN 'Yes'
		ELSE NULL END) as Landmark,

	ownership_translate(
		cityowned,
		ownertype,
		nonprofit
	) as ownership,
	
	ownerlastname||', '||ownerfirstname as owner_name,
	ownerbusinessname as Owner_BizNm,
	ownerhousestreetname as Owner_Address,
	zip as Owner_ZipCode,
	ownerphone as Owner_Phone,

	(CASE WHEN jobtype ~* 'A1|DM' 
		THEN NULLIF(existingheight, '0')
	END)::numeric as Height_Init,

	(CASE WHEN jobtype ~* 'A1|NB' 
		THEN NULLIF(proposedheight, '0')
	END)::numeric as Height_Prop,

	totalconstructionfloorarea as ConstructnSF,

	(CASE 
		WHEN (horizontalenlrgmt = 'Y' AND verticalenlrgmt <> 'Y') 
			THEN 'Horizontal'
		WHEN (horizontalenlrgmt <> 'Y' AND verticalenlrgmt = 'Y') 
			THEN 'Vertical'
		WHEN (horizontalenlrgmt = 'Y' AND verticalenlrgmt = 'Y') 
			THEN 'Horizontal and Vertical'
	END)  as enlargement,

	enlargementsqfootage as EnlargementSF,
	initialcost as CostEstimate,

	(CASE WHEN loftboard = 'Y' THEN 'Yes'
		ELSE NULL END) as LoftBoardCert,

	(CASE WHEN littlee = 'Y' THEN 'Yes'
		WHEN littlee = 'H' THEN 'Yes'
		ELSE NULL END) as eDesignation,

	(CASE WHEN curbcut = 'X' THEN 'Yes'
		ELSE NULL END) as CurbCut,
		
	cluster as TractHomes,
	regexp_replace(
		trim(housenumber), 
		'(^|)0*', '', '') as address_numbr,
	trim(streetname) as address_street,
	regexp_replace(
		trim(housenumber), 
		'(^|)0*', '', '')||' '||trim(streetname) as address,
	bin as bin,
	LEFT(bin, 1)||lpad(block, 5, '0')||lpad(RIGHT(lot,4), 4, '0') as bbl,
	CASE WHEN borough ~* 'Manhattan' THEN '1'
		WHEN borough ~* 'Bronx' THEN '2'
		WHEN borough ~* 'Brooklyn' THEN '3'
		WHEN borough ~* 'Queens' THEN '4'
		WHEN borough ~* 'Staten Island' THEN '5' 
		END as boro,
	-- Add dummy columns for union to now applications for _init_devdb
	existingzoningsqft as zsf_init,
	proposedzoningsqft as zsf_prop,
	NULL::text as zug_init,
	NULL::text as zug_prop,
	NULL::numeric as zsfr_prop,
	NULL::numeric as zsfc_prop,
	NULL::numeric as zsfcf_prop,
	NULL::numeric as zsfm_prop,
	NULL::numeric as prkngprop,
	-- End Dummy columns 
	buildingclass as bldg_class,
	otherdesc as desc_other,
	specialactionstatus as x_withdrawal,
	ST_SetSRID(ST_Point(
		longitude::double precision,
		latitude::double precision),4326) as dob_geom
INTO _INIT_BIS_devdb
FROM dob_jobapplications
WHERE 
	jobdocnumber = '01' AND ( 
		jobtype ~* 'A1|DM|NB' OR (
			jobtype = 'A2' 
			AND sprinkler is NULL 
			AND lower(jobdescription) LIKE '%combin%' 
			AND lower(jobdescription) NOT LIKE '%sprinkler%' 
		)
	) AND gid = 1;


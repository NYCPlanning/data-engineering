/*
DESCRIPTION:
	1. Combine _INIT_BIS_devdb and _INIT_NOW_devdb into _INIT_devdb
	2. Apply corrections on stories_prop, bin, bbl, and date fields

INPUTS:
	_INIT_BIS_devdb
	_INIT_NOW_devdb

OUTPUTS:
	_INIT_devdb (
		id int,
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
		worktypes text,
		x_withdrawal text,
		existingzoningsqft text,
		proposedzoningsqft text,
		zug_init,
		zug_prop,
		buildingclass text,
		otherdesc text,
		zsfr_prop numeric,
		zsfc_prop numeric,
		zsfcf_prop numeric,
		zsfm_prop numeric,
		prkng_prop numeric
	)

*/

DROP TABLE IF EXISTS _init_devdb;
SELECT *
INTO _init_devdb
FROM (
    SELECT
        *,
        'bis' AS datasource
    FROM _init_bis_devdb
    UNION
    SELECT
        *,
        'now' AS datasource
    FROM _init_now_devdb
) AS t;

ALTER TABLE _init_devdb ADD COLUMN id serial CONSTRAINT _init_devdb_pk PRIMARY KEY;
/*
CORRECTIONS:
	stories_prop
	bin
	bbl
	date_lastupdt
	date_filed
	date_statusd
	date_statusp
	date_statusr
	date_statusx
*/
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'stories_prop');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'bin');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'bbl');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'date_lastupdt');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'date_filed');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'date_statusd');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'date_statusp');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'date_statusr');
CALL apply_correction(:'build_schema', '_INIT_devdb', '_manual_corrections', 'date_statusx');

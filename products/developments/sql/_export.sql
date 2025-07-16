-- EXPORT DevDB
DROP TABLE IF EXISTS export_devdb CASCADE;
SELECT *
INTO export_devdb
FROM final_devdb
WHERE
    date_filed::date <= :'CAPTURE_DATE'
    OR date_permittd::date <= :'CAPTURE_DATE'
    OR date_complete::date <= :'CAPTURE_DATE';

-- EXPORT HousingDB
CREATE VIEW export_housing AS
SELECT *
FROM export_devdb
WHERE resid_flag = 'Residential';

-- Switch to 10 char fieldnames
CREATE VIEW shp_devdb AS
SELECT
    job_number AS "Job_Number",
    job_type AS "Job_Type",
    resid_flag AS "ResidFlag",
    nonres_flag AS "NonresFlag",
    job_inactive AS "Job_Inactv",
    job_status AS "Job_Status",
    complete_year AS "CompltYear",
    complete_qrtr AS "CompltQrtr",
    permit_year AS "PermitYear",
    permit_qrtr AS "PermitQrtr",
    classa_init AS "ClassAInit",
    classa_prop AS "ClassAProp",
    classa_net AS "ClassANet",
    classa_hnyaff::numeric AS "ClassA_HNY",
    hotel_init AS "HotelInit",
    hotel_prop AS "HotelProp",
    otherb_init AS "OtherBInit",
    otherb_prop AS "OtherBProp",
    units_co AS "Units_CO",
    boro AS "Boro",
    bin AS "BIN",
    bbl AS "BBL",
    address_numbr AS "AddressNum",
    address_st AS "AddressSt",
    address AS "Address",
    occ_initial AS "Occ_Init",
    occ_proposed AS "Occ_Prop",
    bldg_class AS "Bldg_Class",
    job_desc AS "Job_Desc",
    desc_other AS "Desc_Other",
    date_filed AS "DateFiled",
    date_statusd AS "DateStatsD",
    date_statusp AS "DateStatsP",
    date_permittd AS "DatePermit",
    date_statusr AS "DateStatsR",
    date_statusx AS "DateStatsX",
    date_lastupdt AS "DateLstUpd",
    date_complete AS "DateComplt",
    zoningdist1 AS "ZoningDst1",
    zoningdist2 AS "ZoningDst2",
    zoningdist3 AS "ZoningDst3",
    specialdist1 AS "SpeclDst1",
    specialdist2 AS "SpeclDst2",
    landmark AS "Landmark",
    zsf_init AS "ZSF_Init",
    zsf_prop AS "ZSF_Prop",
    zug_init AS "ZUG_Init",
    zug_prop AS "ZUG_Prop",
    zsfr_prop AS "ZSFR_Prop",
    zsfc_prop AS "ZSFC_Prop",
    zsfcf_prop AS "ZSFCF_Prop",
    zsfm_prop AS "ZSFM_Prop",
    prkng_init AS "Prkng_Init",
    prkng_prop AS "Prkng_Prop",
    stories_init AS "FloorsInit",
    stories_prop AS "FloorsProp",
    height_init AS "HeightInit",
    height_prop AS "HeightProp",
    constructnsf AS "CnstrctnSF",
    enlargement AS "Enlargemnt",
    enlargementsf AS "EnlrgSF",
    costestimate AS "CostEst",
    loftboardcert AS "LoftBoard",
    edesignation AS "eDesigntn",
    curbcut AS "CurbCut",
    tracthomes AS "TractHomes",
    ownership AS "Ownership",
    owner_name AS "OwnrName",
    owner_address AS "OwnrAddr",
    owner_zipcode AS "OwnrZip",
    owner_phone AS "OwnrPhone",
    pluto_unitres AS "PL_UnitRes",
    pluto_bldgsf AS "PL_BldgSF",
    pluto_comsf AS "PL_ComSF",
    pluto_offcsf AS "PL_OffcSF",
    pluto_retlsf AS "PL_RetlSF",
    pluto_ressf AS "PL_ResSF",
    pluto_yrbuilt AS "PL_YrBuilt",
    pluto_yralt1 AS "PL_YrAlt1",
    pluto_yralt2 AS "PL_YrAlt2",
    pluto_histdst AS "PL_Histdst",
    pluto_landmk AS "PL_Landmk",
    pluto_bldgcls AS "PL_BldgCls",
    pluto_landuse AS "PL_LandUse",
    pluto_owner AS "PL_Owner",
    pluto_owntype AS "PL_OwnType",
    pluto_condo AS "PL_Condo",
    pluto_bldgs AS "PL_Bldgs",
    pluto_floors AS "PL_Floors",
    pluto_version AS "PL_Version",
    cenblock2020 AS "CenBlock20",
    centract2020 AS "CenTract20",
    bctcb2020 AS "BCTCB2020",
    bct2020 AS "BCT2020",
    nta2020 AS "NTA2020",
    ntaname2020 AS "NTAName20",
    cdta2020 AS "CDTA2020",
    cdtaname2020 AS "CDTAName20",
    comunitydist AS "CommntyDst",
    councildist AS "CouncilDst",
    schoolsubdist AS "SchSubDist",
    schoolcommnty AS "SchCommnty",
    schoolelmntry AS "SchElmntry",
    schoolmiddle AS "SchMiddle",
    firecompany AS "FireCmpany",
    firebattalion AS "FireBattln",
    firedivision AS "FireDivsn",
    policeprecnct AS "PolicePcnt",
    -- DEPDrainArea as "DEPDrainAr",
    -- DEPPumpStatn as "DEPPumpStn",
    pluto_firm07 AS "PL_FIRM07",
    pluto_pfirm15 AS "PL_PFIRM15",
    latitude AS "Latitude",
    longitude AS "Longitude",
    datasource AS "DataSource",
    geomsource AS "GeomSource",
    dcpeditfields AS "DCPEdited",
    hny_id AS "HNY_ID",
    hny_jobrelate AS "HNY_Relate",
    version AS "Version",
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
FROM export_devdb;

CREATE VIEW shp_housing AS
SELECT *
FROM shp_devdb
WHERE "ResidFlag" = 'Residential';


-- internal project-level files
-- created for distribution on m drive
CREATE VIEW housingdb_post2010_inactive_included_internal AS
SELECT :internal_columns
FROM shp_housing
WHERE "CompltYear"::integer >= '2010'::integer OR ("CompltYear" IS NULL AND "DateLstUpd"::date >= '2010-01-01');

CREATE VIEW housingdb_post2010_internal AS
SELECT *
FROM housingdb_post2010_inactive_included_internal
WHERE "Job_Inactv" IS NULL;


-- external project-level files
-- created for distribution via BYTES by GIS team
CREATE VIEW housingdb_post2010_external AS
SELECT :external_columns
FROM housingdb_post2010_internal;

CREATE VIEW housingdb_post2010_inactive_included_external AS
SELECT :external_columns
FROM housingdb_post2010_inactive_included_internal

-- EXPORT DevDB
DROP TABLE IF EXISTS EXPORT_devdb CASCADE;
SELECT * 
INTO EXPORT_devdb
FROM FINAL_devdb
WHERE (Date_Complete::date <=  :'CAPTURE_DATE'
    OR (Date_Complete IS NULL  
        AND Date_Permittd::date <=  :'CAPTURE_DATE')
    OR (Date_Complete IS NULL 
        AND Date_Permittd IS NULL 
        AND Date_Filed::date <=  :'CAPTURE_DATE'));

-- EXPORT HousingDB
CREATE VIEW export_housing AS 
SELECT * 
FROM EXPORT_devdb
WHERE resid_flag = 'Residential';

-- Switch to 10 char fieldnames
CREATE VIEW SHP_devdb AS
SELECT
    Job_Number as "Job_Number",
    Job_Type as "Job_Type",
    Resid_Flag as "ResidFlag",
    Nonres_Flag as "NonresFlag",
    Job_Inactive as "Job_Inactv",
    Job_Status as "Job_Status",
    Complete_Year as "CompltYear",
    Complete_Qrtr as "CompltQrtr",
    Permit_Year as "PermitYear",
    Permit_Qrtr as "PermitQrtr",
    ClassA_Init as "ClassAInit",
    ClassA_Prop as "ClassAProp",
    ClassA_Net as "ClassANet",
    ClassA_HNYAff::NUMERIC as "ClassA_HNY",
    Hotel_Init as "HotelInit",
    Hotel_Prop as "HotelProp",
    OtherB_Init as "OtherBInit",
    OtherB_Prop as "OtherBProp",
    units_co as "Units_CO",
    Boro as "Boro",
    BIN as "BIN",
    BBL as "BBL",
    Address_Numbr as "AddressNum",
    Address_St as "AddressSt",
    Address as "Address",
    Occ_Initial as "Occ_Init",
    Occ_Proposed as "Occ_Prop",
    Bldg_Class as "Bldg_Class",
    Job_Desc as "Job_Desc",
    Desc_Other as "Desc_Other",
    Date_Filed as "DateFiled",
    Date_StatusD as "DateStatsD",
    Date_StatusP as "DateStatsP",
    Date_Permittd as "DatePermit",
    Date_StatusR as "DateStatsR",
    Date_StatusX as "DateStatsX",
    Date_LastUpdt as "DateLstUpd",
    Date_Complete as "DateComplt",
    ZoningDist1 as "ZoningDst1",
    ZoningDist2 as "ZoningDst2",
    ZoningDist3 as "ZoningDst3",
    SpecialDist1 as "SpeclDst1",
    SpecialDist2 as "SpeclDst2",
    Landmark as "Landmark",
    ZSF_Init as "ZSF_Init",
    ZSF_Prop as "ZSF_Prop",
    ZUG_init as "ZUG_Init",
    ZUG_prop as "ZUG_Prop",
    ZSFR_Prop as "ZSFR_Prop",
    ZSFC_Prop as "ZSFC_Prop",
    ZSFCF_Prop as "ZSFCF_Prop",
    ZSFM_Prop as "ZSFM_Prop",
    PrkngProp as "PrkngProp",
    Stories_Init as "FloorsInit",
    Stories_Prop as "FloorsProp",
    Height_Init as "HeightInit",
    Height_Prop as "HeightProp",
    ConstructnSF as "CnstrctnSF",
    Enlargement as "Enlargemnt",
    EnlargementSF as "EnlrgSF",
    CostEstimate as "CostEst",
    LoftBoardCert as "LoftBoard",
    eDesignation as "eDesigntn",
    CurbCut as "CurbCut",
    TractHomes as "TractHomes",
    Ownership as "Ownership",
    owner_name as "OwnrName",
    Owner_Address as "OwnrAddr",
    Owner_ZipCode as "OwnrZip",
    Owner_Phone as "OwnrPhone",
    PLUTO_UnitRes as "PL_UnitRes",
    PLUTO_BldgSF as "PL_BldgSF",
    PLUTO_ComSF as "PL_ComSF",
    PLUTO_OffcSF as "PL_OffcSF",
    PLUTO_RetlSF as "PL_RetlSF",
    PLUTO_ResSF as "PL_ResSF",
    PLUTO_YrBuilt as "PL_YrBuilt",
    PLUTO_YrAlt1 as "PL_YrAlt1",
    PLUTO_YrAlt2 as "PL_YrAlt2",
    PLUTO_Histdst as "PL_Histdst",
    PLUTO_Landmk as "PL_Landmk",
    PLUTO_BldgCls as "PL_BldgCls",
    PLUTO_LandUse as "PL_LandUse",
    PLUTO_Owner as "PL_Owner",
    PLUTO_OwnType as "PL_OwnType",
    PLUTO_Condo as "PL_Condo",
    PLUTO_Bldgs as "PL_Bldgs",
    PLUTO_Floors as "PL_Floors",
    PLUTO_Version as "PL_Version",
    CenBlock2020 as "CenBlock20",
    CenTract2020 as "CenTract20",
    BCTCB2020 as "BCTCB2020",
    BCT2020 as "BCT2020",
    NTA2020 as "NTA2020",
    NTAName2020 as "NTAName20",
    CDTA2020 as "CDTA2020",
    CDTAName2020 as "CDTAName20",
    ComunityDist as "CommntyDst",
    CouncilDist as "CouncilDst",
    SchoolSubDist as "SchSubDist",
    SchoolCommnty as "SchCommnty",
    SchoolElmntry as "SchElmntry",
    SchoolMiddle as "SchMiddle",
    FireCompany as "FireCmpany",
    FireBattalion as "FireBattln",
    FireDivision as "FireDivsn",
    PolicePrecnct as "PolicePcnt",
    -- DEPDrainArea as "DEPDrainAr",
    -- DEPPumpStatn as "DEPPumpStn",
    PLUTO_FIRM07 as "PL_FIRM07",
    PLUTO_PFIRM15 as "PL_PFIRM15",
    Latitude as "Latitude",
    Longitude as "Longitude",
    DataSource as "DataSource",
    GeomSource as "GeomSource",
    DCPEditFields as "DCPEdited",
    HNY_ID as "HNY_ID",
    HNY_JobRelate as "HNY_Relate",
    Version as "Version",
    ST_SetSRID(ST_MakePoint(longitude,latitude), 4326) as geom
FROM EXPORT_devdb;

CREATE VIEW shp_housing AS 
SELECT * 
FROM SHP_devdb
WHERE "ResidFlag" = 'Residential';


-- internal project-level files
-- created for distribution on m drive
CREATE VIEW HousingDB_post2010_all_internal AS 
SELECT :internal_columns
FROM shp_housing
WHERE "CompltYear"::integer >= '2010'::integer OR ("CompltYear" IS NULL AND "DateLstUpd"::DATE >= '2010-01-01');

CREATE VIEW HousingDB_post2010_internal AS
SELECT *
FROM HousingDB_post2010_all_internal
WHERE "Job_Inactv" IS NULL;

CREATE VIEW HousingDB_post2010_completed_jobs_internal AS
SELECT *
FROM HousingDB_post2010_internal
WHERE "CompltYear"::integer >= '2010'::integer;

CREATE VIEW HousingDB_post2010_incomplete_jobs_internal AS
SELECT *
FROM HousingDB_post2010_internal
WHERE "CompltYear" IS NULL;

CREATE VIEW HousingDB_post2010_inactive_jobs_internal AS
SELECT *
FROM HousingDB_post2010_all_internal
WHERE "Job_Inactv" IS NOT NULL;

CREATE VIEW HousingDB_post2010_inactive_included_internal AS
SELECT *
FROM HousingDB_post2010_all_internal;


-- external project-level files
-- created for distribution via BYTES by GIS team
CREATE VIEW HousingDB_post2010_all_external AS 
SELECT :external_columns
FROM HousingDB_post2010_all_internal;

CREATE VIEW HousingDB_post2010 AS
SELECT :external_columns
FROM HousingDB_post2010_internal;

CREATE VIEW HousingDB_post2010_completed_jobs_external AS
SELECT :external_columns
FROM HousingDB_post2010_completed_jobs_internal;

CREATE VIEW HousingDB_post2010_incomplete_jobs_external AS
SELECT :external_columns
FROM HousingDB_post2010_incomplete_jobs_internal;

CREATE VIEW HousingDB_post2010_inactive_jobs_external AS
SELECT :external_columns
FROM HousingDB_post2010_inactive_jobs_internal;

CREATE VIEW HousingDB_post2010_inactive_included_external AS
SELECT :external_columns
FROM HousingDB_post2010_inactive_included_internal

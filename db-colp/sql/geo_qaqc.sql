DROP TABLE IF EXISTS ipis_colp_georesults;
SELECT
    a.*,
    b."HNUM" as display_hnum,
    b."AGENCY",
    b."PARCELNAME",
    b."USECODE",
    b."USETYPE",
    b."OWNERSHIP",
    b."CATEGORY",
    b."EXPANDCAT",
    b."EXCATDESC",
    b."LEASED",
    b."FINALCOM",
    b."AGREEMENT"
INTO ipis_colp_georesults
FROM geo_qaqc a
JOIN _colp b
ON a.uid = b.uid
;
DROP TABLE IF EXISTS colp;
SELECT
    uid::varchar(32) AS uid,
    "BOROUGH",
    "BLOCK",
    "LOT",
    "BBL",
    "MAPBBL",
    "CD",
    "HNUM",
    "SNAME",
    (CASE
        WHEN "HNUM" IS NOT NULL AND "SNAME" <> ''
            THEN CONCAT("HNUM", ' ', "SNAME")
        ELSE "SNAME"
    END) AS "ADDRESS",
    "PARCELNAME",
    "AGENCY",
    "USECODE",
    "USETYPE",
    "OWNERSHIP",
    "CATEGORY",
    "EXPANDCAT",
    "EXCATDESC",
    "LEASED",
    "FINALCOM",
    "AGREEMENT",
    "XCOORD",
    "YCOORD",
    "LATITUDE",
    "LONGITUDE",
    "DCPEDITED",
    "GEOM"
INTO colp
FROM _colp
WHERE "XCOORD" IS NOT NULL;

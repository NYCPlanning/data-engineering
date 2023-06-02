/*
GEO SPATIAL QAQC
*/

DROP TABLE IF EXISTS GEO_qaqc;
SELECT
    job_number,
    CASE WHEN in_water(geom)
        THEN 1 ELSE 0 END
    AS geo_water,
    CASE 
        WHEN get_bbl(geom) IS NULL 
        THEN 1 ELSE 0 END 
    AS geo_taxlot,
    CASE 
        WHEN longitude is null 
            OR latitude is null 
            THEN 1 else 0 end 
    AS geo_null_latlong,
    CASE 
        WHEN geo_zipcode IS NULL OR
            geo_boro IS NULL OR
            geo_cd IS NULL OR
            geo_council IS NULL OR
            geo_nta2010 IS NULL OR
            geo_ntaname2010 IS NULL OR
            geo_cb2010 IS NULL OR
            geo_ct2010 IS NULL OR
            geo_nta2020 IS NULL OR
            geo_ntaname2020 IS NULL OR
            geo_cb2020 IS NULL OR
            geo_ct2020 IS NULL OR
            geo_cdta2020 IS NULL OR
            geo_cdtaname2020 IS NULL OR
            geo_csd IS NULL OR
            geo_policeprct IS NULL OR
            geo_firedivision IS NULL OR
            geo_firebattalion IS NULL OR
            geo_firecompany IS NULL OR
            geo_schoolelmntry IS NULL OR
            geo_schoolmiddle IS NULL OR
            geo_schoolsubdist IS NULL
        THEN 1 ELSE 0 END 
    AS geo_null_boundary
INTO GEO_qaqc
FROM MID_devdb;
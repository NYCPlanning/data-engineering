CREATE TEMP TABLE tmp as (
    SELECT 
        geoid, 
        st_centroid(wkb_geometry) as centroid
    FROM (
        (select * FROM uscensus_ct_shp."2019/09/17")
        UNION 
        (select * FROM uscensus_ny_shp."2019/09/17")
        UNION
        (select * FROM uscensus_nj_shp."2019/09/17")
    ) a
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;
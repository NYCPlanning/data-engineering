CREATE TEMP TABLE tmp (
    enddesc text,
    muni text,
    tdv_route text,
    aadt_year text,
    data_type text,
    vol_txt text,
    class_txt text,
    speed_txt text,
    vol_tdv text,
    class_tdv text,
    speed_tdv text,
    "shape.stlength()" double precision,
    rc_id text,
    loc_error text,
    objectid integer,
    aadt integer,
    objectid_1 integer,
    ccst text,
    begdesc text,
    fc text,
    perc_truck double precision,
    count_type text,
    su_aadt integer,
    cu_aadt integer,
    countyr text,
    aadt_last_ integer,
    gis_id text,
    firstofbeg double precision,
    lastofend_ double precision,
    geom geometry(MultiLineString,4326)
);

\COPY tmp FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT *
INTO :NAME.:"VERSION"
FROM tmp;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);
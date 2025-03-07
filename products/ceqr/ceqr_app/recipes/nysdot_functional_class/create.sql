CREATE TEMP TABLE tmp (
    objectid integer,
    route_no text,
    func_class integer,
    segment_name text,
    unique_id integer,
    roadway_type double precision,
    co_rd_no text,
    access_control double precision,
    bridge_feature_number text,
    hpms_sample_id double precision,
    jurisdiction text,
    last_actual_cntyr double precision,
    median_type text,
    mpo text,
    municipality_type text,
    borough text,
    owning_juris text,
    ramp_dest_co_order double precision,
    ramp_orig_co_order double precision,
    shoulder_type text,
    strahnet text,
    surface_type double precision,
    tandem_truck double precision,
    toll text,
    urban_area_code text,
    owned_by_muni_type text,
    from_date text,
    to_date text,
    locerror text,
    "shape.stlength()" double precision,
    signing text,
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

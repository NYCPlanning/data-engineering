/*
DESCRIPTION: 
    Taking all fields from nysdot_functional_class that are 
    spatially within NYC water included borough boundaries

INPUT:
    nysdot_functional_class (
        ogc_fid                 integer,
        objectid                integer,
        route_no                character varying,
        func_class              integer,
        segment_name            character varying,
        unique_id               integer,
        roadway_type            double precision,
        co_rd_no                character varying,
        access_control          double precision,
        bridge_feature_number   character varying,
        hpms_sample_id          double precision,
        jurisdiction            character varying,
        last_actual_cntyr       double precision,
        median_type             character varying,
        mpo                     character varying,
        municipality_type       character varying,
        municipality_desc       character varying,
        owning_juris            character varying,
        ramp_dest_co_order      double precision,
        ramp_orig_co_order      double precision,
        shoulder_type           character varying,
        strahnet                character varying,
        surface_type            double precision,
        tandem_truck            double precision,
        toll                    character varying,
        urban_area_code         character varying,
        owned_by_muni_type      character varying,
        from_date               character varying,
        to_date                 character varying,
        locerror                character varying,
        "shape.stlength()"      double precision,
        signing                 character varying,
        wkb_geometry            geometry(MultiLineString,4326)
    )
OUTPUT:
    TEMP tmp (
        all fields in nysdot_functional_class
        wkb_geometry rename to geom
    )
*/
CREATE TEMP TABLE tmp as (
    WITH draft as (
        SELECT 
            *, 
            municipality_desc AS borough, 
            wkb_geometry AS geom
        FROM nysdot_functional_class.latest c
        WHERE wkb_geometry IS NOT NULL 
        AND c.ogc_fid IN (
            SELECT a.ogc_fid FROM
            nysdot_functional_class.latest a, (
                SELECT ST_Union(wkb_geometry) As wkb_geometry
                FROM dcp_boroboundaries_wi.latest
            ) b
            WHERE ST_Contains(b.wkb_geometry, a.wkb_geometry)
            OR ST_Intersects(b.wkb_geometry, a.wkb_geometry)
        )
    ) 
    SELECT 
        a.objectid,
        a.route_no,
        a.func_class,
        a.segment_name,
        a.unique_id,
        a.roadway_type,
        a.co_rd_no,
        a.access_control,
        a.bridge_feature_number,
        a.hpms_sample_id,
        a.jurisdiction,
        a.last_actual_cntyr,
        a.median_type,
        a.mpo,
        a.municipality_type,
        a.borough,
        a.owning_juris,
        a.ramp_dest_co_order,
        a.ramp_orig_co_order,
        a.shoulder_type,
        a.strahnet,
        a.surface_type,
        a.tandem_truck,
        a.toll,
        a.urban_area_code,
        a.owned_by_muni_type,
        a.from_date,
        a.to_date,
        a.locerror,
        a."shape.stlength()",
        a.signing,
        a.geom
    FROM draft a
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;

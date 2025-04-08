DROP TABLE IF EXISTS geospatial_check;
CREATE TABLE IF NOT EXISTS geospatial_check (
    result character varying
);

INSERT INTO geospatial_check (
    SELECT jsonb_agg(t) AS result
    FROM (
        SELECT
            jsonb_agg(json_build_object(
                'uid', tmp.uid, 'hnum', tmp.hnum, 'sname', tmp.sname,
                'adress', tmp.address, 'parcelname', tmp.parcelname, 'agency', tmp.agency,
                'latitude', tmp.latitude, 'longitude', tmp.longitude, 'v', tmp.v
            )) AS values,
            'projects_not_within_NYC' AS field
        FROM (
            SELECT
                a.uid,
                a.hnum,
                a.sname,
                a.address,
                a.parcelname,
                a.agency,
                a.latitude,
                a.longitude,
                a.data_library_version AS v
            FROM dcp_colp AS a,
                (
                    SELECT st_union(wkb_geometry) AS geom
                    FROM dcp_boroboundaries_wi
                ) AS combined
            WHERE NOT st_within(a.geom, combined.geom)
        ) AS tmp
    ) AS t
);

INSERT INTO geospatial_check (
    SELECT jsonb_agg(t) AS result
    FROM (
        SELECT
            jsonb_agg(json_build_object(
                'uid', tmp.uid, 'borough', tmp.borough, 'cd', tmp.cd,
                'bbl', tmp.bbl, 'v', tmp.v
            )) AS values,
            'projects_inconsistent_geographies' AS field
        FROM (
            SELECT
                uid,
                borough,
                cd,
                bbl,
                data_library_version AS v
            FROM dcp_colp
            WHERE
                borough <> left(cd::text, 1)
                OR borough <> left(bbl::text, 1)
        ) AS tmp
    ) AS t
);

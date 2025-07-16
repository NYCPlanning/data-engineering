DROP TABLE IF EXISTS geospatial_check;
CREATE TABLE IF NOT EXISTS geospatial_check (
    v character varying,
    result character varying
);

DELETE FROM geospatial_check
WHERE v = :'ccp_v';

INSERT INTO geospatial_check (
    SELECT
        :'ccp_v' AS v,
        jsonb_agg(t) AS result
    FROM (
        SELECT
            jsonb_agg(json_build_object(
                'projectid', tmp.projectid, 'magencyacro', tmp.magencyacro,
                'typecategory', tmp.typecategory,
                'description', tmp.description
            )) AS values,
            'projects_not_within_NYC' AS field
        FROM (
            SELECT
                a.projectid,
                a.magencyacro,
                a.typecategory,
                a.description
            FROM cpdb_dcpattributes AS a,
                (
                    SELECT ST_Union(wkb_geometry) AS geom
                    FROM dcp_boroboundaries_wi
                ) AS combined
            WHERE NOT ST_Within(a.geom, combined.geom)
        ) AS tmp
    ) AS t
)

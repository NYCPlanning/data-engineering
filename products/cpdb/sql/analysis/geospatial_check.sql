DROP TABLE IF EXISTS geospatial_check;
CREATE TABLE IF NOT EXISTS geospatial_check (
    result character varying
);

INSERT INTO geospatial_check (
    SELECT jsonb_agg(t) AS result
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
                    SELECT st_union(wkb_geometry) AS geom
                    FROM dcp_boroboundaries_wi
                ) AS combined
            WHERE NOT st_within(a.geom, combined.geom)
        ) AS tmp
    ) AS t
)

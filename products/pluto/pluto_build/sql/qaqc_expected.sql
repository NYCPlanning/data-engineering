-- -v VERSION=$VERSION
DELETE FROM qaqc_expected
WHERE v = :'VERSION';

INSERT INTO qaqc_expected (
    SELECT
        :'VERSION' AS v,
        jsonb_agg(t) AS expected
    FROM (
        SELECT
            jsonb_agg(zonedist1) AS values,
            'zonedist1' AS field
        FROM (SELECT DISTINCT zonedist1 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(zonedist2) AS values,
            'zonedist2' AS field
        FROM (SELECT DISTINCT zonedist2 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(zonedist3) AS values,
            'zonedist3' AS field
        FROM (SELECT DISTINCT zonedist3 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(zonedist4) AS values,
            'zonedist4' AS field
        FROM (SELECT DISTINCT zonedist4 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(overlay1) AS values,
            'overlay1' AS field
        FROM (SELECT DISTINCT overlay1 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(overlay2) AS values,
            'overlay2' AS field
        FROM (SELECT DISTINCT overlay2 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(spdist1) AS values,
            'spdist1' AS field
        FROM (SELECT DISTINCT spdist1 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(spdist2) AS values,
            'spdist2' AS field
        FROM (SELECT DISTINCT spdist2 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(spdist3) AS values,
            'spdist3' AS field
        FROM (SELECT DISTINCT spdist3 FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(ext) AS values,
            'ext' AS field
        FROM (SELECT DISTINCT ext FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(proxcode) AS values,
            'proxcode' AS field
        FROM (SELECT DISTINCT proxcode FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(irrlotcode) AS values,
            'irrlotcode' AS field
        FROM (SELECT DISTINCT irrlotcode FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(lottype) AS values,
            'lottype' AS field
        FROM (SELECT DISTINCT lottype FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(bsmtcode) AS values,
            'bsmtcode' AS field
        FROM (SELECT DISTINCT bsmtcode FROM archive_pluto) AS a
        UNION
        SELECT
            jsonb_agg(bldgclasslanduse) AS values,
            'bldgclasslanduse' AS field
        FROM (SELECT DISTINCT bldgclass || '/' || landuse AS bldgclasslanduse FROM archive_pluto) AS a
    ) AS t
);

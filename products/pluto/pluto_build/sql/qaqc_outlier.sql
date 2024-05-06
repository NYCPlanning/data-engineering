DELETE FROM qaqc_outlier
WHERE
    v = :'VERSION'
    AND condo::boolean = :CONDO
    AND mapped::boolean = :MAPPED;

INSERT INTO qaqc_outlier (
    SELECT
        :'VERSION' AS v,
        :CONDO AS condo,
        :MAPPED AS mapped,
        jsonb_agg(t) AS outlier
    FROM (
        SELECT
            jsonb_agg(json_build_object(
                'bbl', tmp.bbl,
                'unitsres', tmp.unitsres,
                'resarea', tmp.resarea,
                'res_unit_ratio', tmp.res_unit_ratio
            )) AS values,
            'unitsres_resarea' AS field
        FROM (
            SELECT
                bbl,
                unitsres,
                resarea,
                resarea::float / unitsres::float AS res_unit_ratio
            FROM (
                SELECT a.*
                FROM archive_pluto AS a
                :CONDITION
            ) AS b
            WHERE
                unitsres::float > 50 AND resarea::float / unitsres::float < 300
                AND resarea::float != 0
        ) AS tmp
        UNION
        SELECT
            jsonb_agg(json_build_object(
                'bbl', tmp.bbl,
                'bldgarea', tmp.bldgarea,
                'lotarea', tmp.lotarea,
                'numfloors', tmp.numfloors,
                'blog_lot_ratio', tmp.bldg_lot_ratio
            )) AS values,
            'lotarea_numfloor' AS field
        FROM (
            SELECT
                bbl,
                bldgarea,
                lotarea,
                numfloors,
                bldgarea::float / lotarea::float AS bldg_lot_ratio
            FROM (
                SELECT a.*
                FROM archive_pluto AS a
                :CONDITION
            ) AS b
            WHERE
                bldgarea::float / lotarea::float > numfloors::float * 2
                AND lotarea::float != 0 AND numfloors != 0
        ) AS tmp
        UNION
        SELECT
            jsonb_agg(json_build_object(
                'pair', m.pair, 'bbl', m.bbl,
                'building_area_current', m.building_area_current,
                'building_area_previous', m.building_area_previous,
                'percent_change', m.percent_change
            )) AS values,
            'building_area_increase' AS field
        FROM (
            SELECT
                :'VERSION' || ' - ' || :'VERSION_PREV' AS pair,
                a.bbl,
                a.lotarea::float AS building_area_current,
                b.lotarea::float AS building_area_previous,
                ((a.lotarea::float - b.lotarea::float) / b.lotarea::float * 100) AS percent_change
            FROM (
                SELECT a.*
                FROM archive_pluto AS a
                :CONDITION
            ) AS a, previous_pluto AS b
            WHERE
                a.bbl::float = b.bbl::float AND a.lotarea::float > 2 * b.lotarea::float
                AND b.lotarea::float != 0
        ) AS m
    ) AS t
);

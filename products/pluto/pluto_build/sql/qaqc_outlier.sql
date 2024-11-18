DELETE FROM qaqc_outlier
WHERE
    v = :'VERSION'
    AND condo::boolean = :CONDO
    AND mapped::boolean = :MAPPED;


WITH pluto_filtered AS (
    SELECT *
    FROM archive_pluto AS a
    :CONDITION
),
-- building area has more than doubled in size
building_area_increase AS (
    SELECT
        jsonb_agg(json_build_object(
            'pair', :'VERSION' || ' - ' || :'VERSION_PREV',
            'bbl', current.bbl,
            'building_area_current', current.bldgarea::float,
            'building_area_previous', prev.bldgarea::float,
            'percent_change', (current.bldgarea::float - prev.bldgarea::float) / prev.bldgarea::float * 100
        )) AS values,
        'building_area_increase' AS field
    FROM pluto_filtered AS current
    INNER JOIN previous_pluto AS prev ON current.bbl::float = prev.bbl::float
    WHERE
        current.bldgarea::float > 2 * prev.bldgarea::float
        AND prev.bldgarea::float != 0
),
-- unreasonably small apartments based on area/unit ratio
small_apartments AS (
    SELECT
        jsonb_agg(json_build_object(
            'bbl', bbl,
            'ownername', ownername,
            'unitsres', unitsres,
            'resarea', resarea,
            'res_unit_ratio', resarea::float / unitsres::float
        )) AS values,
        'unitsres_resarea' AS field
    FROM pluto_filtered
    WHERE
        unitsres::float > 50 AND resarea::float / unitsres::float < 300
        AND resarea::float != 0
),
-- building area over lot area is more than double the number of floors
floors AS (
    SELECT
        jsonb_agg(json_build_object(
            'bbl', current.bbl,
            'bldgarea', current.bldgarea,
            'lotarea', current.lotarea,
            'numfloors', current.numfloors,
            'blog_lot_ratio', current.bldgarea::float / current.lotarea::float,
            'new_flag', prev.bbl IS NULL
        )) AS values,
        'lotarea_numfloor' AS field
    FROM pluto_filtered AS current
    LEFT JOIN previous_pluto AS prev
        ON
            current.bbl::float = prev.bbl::float
            AND prev.lotarea::float != 0
            AND prev.numfloors::float != 0
            AND prev.bldgarea::float / prev.lotarea::float > prev.numfloors::float * 2
    WHERE
        current.bldgarea::float / current.lotarea::float > current.numfloors::float * 2
        AND current.lotarea::float != 0 AND current.numfloors != 0
),
unioned AS (
    SELECT * FROM building_area_increase
    UNION ALL
    SELECT * FROM small_apartments
    UNION ALL
    SELECT * FROM floors
)
INSERT INTO qaqc_outlier (
    SELECT
        :'VERSION' AS v,
        :CONDO AS condo,
        :MAPPED AS mapped,
        jsonb_agg(unioned) AS outlier
    FROM unioned
);

/**********************************************************************************************************************************************************************************
Sources kpdb - finalized version of KPDB build
         dcp_school_districts
OUTPUT: longform_csd_output
*************************************************************************************************************************************************************************************/

DROP TABLE IF EXISTS aggregated_csd;
DROP TABLE IF EXISTS ungeocoded_projects_csd;
DROP TABLE IF EXISTS aggregated_csd_longform;
DROP TABLE IF EXISTS aggregated_csd_project_level;
DROP TABLE IF EXISTS longform_csd_output;


SELECT *
INTO
aggregated_csd
FROM (
    WITH aggregated_boundaries_csd AS (
        SELECT
            --a.cartodb_id,
            a.geometry,
            --a.geometry_webmercator,
            a.project_id,
            a.source,
            a.record_id,
            a.record_name,
            a.borough,
            a.status,
            a.type,
            a.date,
            a.date_type,
            a.units_gross,
            a.units_net,
            a.prop_within_5_years,
            a.prop_5_to_10_years,
            a.prop_after_10_years,
            a.within_5_years,
            a.from_5_to_10_years,
            a.after_10_years,
            a.phasing_rationale,
            a.phasing_known,
            a.nycha,
            a.classb,
            a.senior_housing,
            a.inactive,
            b.geometry AS csd_geom,
            b.schooldist AS csd,
            ST_Distance(
                a.geometry::geography, b.geometry::geography
            ) AS csd_distance
        FROM
            -- capitalplanning.kpdb_2021_09_10_nonull a
            kpdb AS a
        LEFT JOIN
            dcp_school_districts AS b
            ON
                CASE
                    /*Treating large developments as polygons*/
                    WHEN
                        (
                            ST_Area(a.geometry::geography) > 10000
                            OR units_gross > 500
                        )
                        AND a.source IN (
                            'EDC Projected Projects',
                            'DCP Application',
                            'DCP Planner-Added Projects'
                        )
                        THEN
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            ST_Intersects(a.geometry, b.geometry)
                            AND (ST_Area(
                                ST_Intersection(a.geometry, b.geometry)
                            )
                            / ST_Area(a.geometry))::decimal
                            >= .1

                    /*Treating subdivisions in SI across many lots as polygons*/
                    WHEN
                        a.record_id IN (
                            SELECT record_id FROM zap_project_many_bbls
                        )
                        AND a.record_name LIKE '%SD %'
                        THEN
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            ST_Intersects(a.geometry, b.geometry)
                            AND (ST_Area(
                                ST_Intersection(a.geometry, b.geometry)
                            )
                            / ST_Area(a.geometry))::decimal
                            >= .1

                    /*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/
                    WHEN
                        a.record_name LIKE '%Resilient Housing%'
                        AND a.source IN (
                            'DCP Application', 'DCP Planner-Added PROJECTs'
                        )
                        THEN
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            ST_Intersects(a.geometry, b.geometry)
                            AND (ST_Area(
                                ST_Intersection(a.geometry, b.geometry)
                            )
                            / ST_Area(a.geometry))::decimal
                            >= .1

                    /*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/
                    WHEN
                        (
                            a.record_name LIKE '%NIHOP%'
                            OR a.record_name LIKE '%NCP%'
                        )
                        AND a.source IN (
                            'DCP Application', 'DCP Planner-Added PROJECTs'
                        )
                        THEN
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            ST_Intersects(a.geometry, b.geometry)
                            AND (ST_Area(
                                ST_Intersection(a.geometry, b.geometry)
                            )
                            / ST_Area(a.geometry))::decimal
                            >= .1

                    /*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
                    WHEN
                        a.source IN (
                            'Future Neighborhood Studies',
                            'Neighborhood Study Projected Development Sites'
                        )
                        THEN
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            ST_Intersects(a.geometry, b.geometry)
                            AND (ST_Area(
                                ST_Intersection(a.geometry, b.geometry)
                            )
                            / ST_Area(a.geometry))::decimal
                            >= .1
                    /*Treating other polygons as points, using their centroid*/

                    /*Treating other polygons as points, using their centroid*/
                    WHEN ST_Area(a.geometry) > 0
                        THEN
                            ST_Intersects(ST_Centroid(a.geometry), b.geometry)

                    /*Treating points as points*/
                    ELSE
                        ST_Intersects(a.geometry, b.geometry)
                END
    /*Only matching if at least 10% of the polygon is in the boundary. Otherwise, the polygon will be apportioned to its other boundaries only*/
    ),

    /*Identify projects geocoded to multiple CSDs*/
    multi_geocoded_projects AS (
        SELECT
            source,
            record_id
        FROM
            aggregated_boundaries_csd
        GROUP BY
            source,
            record_id
        HAVING
            count(*) > 1
    ),

    /*Calculate the proportion of each project in each CSD that it overlaps with*/
    aggregated_boundaries_csd_2 AS (
        SELECT
            a.*,
            CASE
                WHEN
                    concat(a.source, a.record_id) IN (
                        SELECT concat(source, record_id)
                        FROM multi_geocoded_projects
                    )
                    AND ST_Area(a.geometry) > 0
                    THEN
                        (
                            ST_Area(ST_Intersection(a.geometry, a.csd_geom))
                            / ST_Area(a.geometry)
                        )::decimal
                ELSE
                    1
            END AS proportion_in_csd
        FROM
            aggregated_boundaries_csd AS a
    ),

    /*
    If <10% of a project falls into a particular CSD, then the sum of all proportions of a project in each CSD would be <100%, because
    projects with less than 10% in a CSD are not assigned to that CSD. The next two steps ensure that 100% of each project's units are
    allocated to a CSD.
    */
    aggregated_boundaries_csd_3 AS (
        SELECT
            source,
            record_id,
            sum(proportion_in_csd) AS total_proportion
        FROM
            aggregated_boundaries_csd_2
        GROUP BY
            source,
            record_id
    ),

    aggregated_boundaries_csd_4 AS (
        SELECT
            a.*,
            CASE
                WHEN
                    b.total_proportion IS NOT NULL
                    THEN (a.proportion_in_csd / b.total_proportion)::decimal
                ELSE 1
            END AS proportion_in_csd_1,
            CASE
                WHEN
                    b.total_proportion IS NOT NULL
                    THEN
                        round(
                            a.units_net
                            * (
                                a.proportion_in_csd
                                / b.total_proportion
                            )::decimal
                        )
                ELSE a.units_net
            END AS counted_units_1
        FROM
            aggregated_boundaries_csd_2 AS a
        LEFT JOIN
            aggregated_boundaries_csd_3 AS b
            ON
                a.record_id = b.record_id AND a.source = b.source
    )

    SELECT * FROM aggregated_boundaries_csd_4
) AS _1;


/*Identify projects which did not geocode to any CSD*/
SELECT *
INTO
ungeocoded_projects_csd
FROM (
    WITH ungeocoded_projects_csd AS (
        SELECT
            a.*,
            coalesce(a.csd, b.schooldist) AS csd_1,
            coalesce(
                a.csd_distance,
                ST_Distance(
                    csd_geom::geography,
                    CASE
                        WHEN
                            (
                                ST_Area(a.geometry::geography) > 10000
                                OR units_gross > 500
                            )
                            AND a.source IN (
                                'DCP Application', 'DCP Planner-Added PROJECTs'
                            )
                            THEN a.geometry::geography
                        WHEN
                            ST_Area(a.geometry) > 0
                            THEN ST_Centroid(a.geometry)::geography
                        ELSE a.geometry::geography
                    END
                )
            ) AS csd_distance1
        FROM
            aggregated_csd AS a
        LEFT JOIN
            dcp_school_districts AS b
            ON
                a.csd_distance IS NULL
                AND CASE
                    WHEN
                        (
                            ST_Area(a.geometry::geography) > 10000
                            OR units_gross > 500
                        )
                        AND a.source IN (
                            'DCP Application', 'DCP Planner-Added PROJECTs'
                        )
                        THEN
                            ST_DWithin(
                                a.geometry::geography, csd_geom::geography, 500
                            )
                    WHEN ST_Area(a.geometry) > 0
                        THEN
                            ST_DWithin(
                                ST_Centroid(a.geometry)::geography,
                                csd_geom::geography,
                                500
                            )
                    ELSE
                        ST_DWithin(
                            a.geometry::geography, csd_geom::geography, 500
                        )
                END
    )

    SELECT * FROM ungeocoded_projects_csd
) AS _2;

/*Assign ungeocoded projects to their closest CSD*/

SELECT *
INTO
aggregated_csd_longform
FROM (
    WITH min_distances AS (
        SELECT
            record_id,
            min(csd_distance1) AS min_distance
        FROM
            ungeocoded_projects_csd
        GROUP BY
            record_id
    ),

    all_projects_csd AS (
        SELECT a.*
        FROM
            ungeocoded_projects_csd AS a
        INNER JOIN
            min_distances AS b
            ON
                a.record_id = b.record_id
                AND a.csd_distance1 = b.min_distance
    )

    SELECT
        a.*,
        b.csd_1 AS csd,
        b.proportion_in_csd_1 AS proportion_in_csd,
        round(a.units_net * b.proportion_in_csd_1) AS units_net_in_csd
    FROM
        -- capitalplanning.kpdb_2021_09_10_nonull a 
        kpdb AS a
    LEFT JOIN
        all_projects_csd AS b
        ON
            a.source = b.source
            AND a.record_id = b.record_id
    ORDER BY
        source ASC,
        record_id ASC,
        record_name ASC,
        status ASC,
        b.csd_1 ASC
) AS _3
ORDER BY csd ASC;

/*Aggregate all results to the project-level, because if a project matches with multiple CSDs, it'll appear in multiple rows*/

SELECT *
INTO
aggregated_csd_project_level
FROM (
    SELECT
        source,
        record_id,
        record_name,
        type,
        inactive,
        status,
        borough,
        units_gross,
        units_net,
        prop_within_5_years,
        prop_5_to_10_years,
        prop_after_10_years,
        within_5_years,
        from_5_to_10_years,
        after_10_years,
        phasing_rationale,
        phasing_known,
        date,
        date_type,
        nycha,
        classb,
        senior_housing,
        geometry,
        array_to_string(
            array_agg(
                nullif(
                    concat_ws(
                        ': ',
                        nullif(concat(csd), ''),
                        concat(round(100 * proportion_in_csd, 0), '%')
                    ),
                    ''
                )
            ),
            ' | '
        ) AS csd
    --geometry_webmercator 
    FROM
        (
            SELECT * FROM aggregated_csd_longform
            ORDER BY csd ASC
        ) AS a
    GROUP BY
        geometry,
        --geometry_webmercator,
        source,
        record_id,
        record_name,
        type,
        inactive,
        status,
        borough,
        units_gross,
        units_net,
        prop_within_5_years,
        prop_5_to_10_years,
        prop_after_10_years,
        within_5_years,
        from_5_to_10_years,
        after_10_years,
        phasing_rationale,
        phasing_known,
        date,
        date_type,
        nycha,
        classb,
        senior_housing
) AS x;

-- this is a bit fragile - if remaining unassigned projects overlapped with multiple, this would have undesired behavior
-- quick fix on 3/29/23 to fix 43 records not matching. Verified via manual querying that this has desired outcome
UPDATE aggregated_csd_longform a
SET
    csd = b.schooldist,
    proportion_in_csd = 1,
    units_net_in_csd = a.units_net
FROM dcp_school_districts AS b
WHERE
    a.csd IS NULL
    AND NOT ST_IsEmpty(a.geometry)
    AND ST_Intersects(a.geometry, b.geometry);

/*
	Output final CSD-based KPDB. This is not at the project-level, but rather the project & CSD-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.


  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations

*/

SELECT *
INTO
longform_csd_output
FROM (
    SELECT * FROM aggregated_csd_longform
    -- where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
    ORDER BY
        source ASC,
        record_id ASC,
        record_name ASC,
        status ASC
) AS x;


-- select cdb_cartodbfytable('capitalplanning','longform_csd_output'); -- not necessary to run next script

-- Drop intermediate tables
DROP TABLE IF EXISTS aggregated_csd;
DROP TABLE IF EXISTS ungeocoded_projects_csd;
DROP TABLE IF EXISTS aggregated_csd_longform;
DROP TABLE IF EXISTS aggregated_csd_project_level;

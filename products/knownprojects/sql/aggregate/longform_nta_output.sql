/**********************************************************************************************************************************************************************************
Sources kpdb_deduplicated - finalized version of KPDB build with no duplicate record_id values
         dcp_nta2020
OUTPUT: longform_nta_output
*************************************************************************************************************************************************************************************/

DROP TABLE IF EXISTS aggregated_nta;
DROP INDEX IF EXISTS aggregated_nta_gix;
DROP TABLE IF EXISTS ungeocoded_projects_nta;
DROP TABLE IF EXISTS aggregated_nta_longform;
DROP TABLE IF EXISTS aggregated_nta_project_level;
DROP TABLE IF EXISTS longform_nta_output;


SELECT *
INTO
aggregated_nta
FROM (
    WITH aggregated_boundaries_nta AS (
        SELECT
            a.geometry,
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
            a.has_project_phasing,
            a.has_future_units,
            a.future_units_without_phasing,
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
            b.geometry AS nta_geom,
            b.nta2020 AS nta,
            ST_Distance(
                a.geometry::geography, b.geometry::geography
            ) AS nta_distance
        FROM
            -- capitalplanning.kpdb_2021_09_10_nonull a
            kpdb_deduplicated AS a
        LEFT JOIN
            dcp_nta2020 AS b
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

    /*Identify projects geocoded to multiple Boundarys*/
    multi_geocoded_projects AS (
        SELECT
            source,
            record_id
        FROM
            aggregated_boundaries_nta
        GROUP BY
            source,
            record_id
        HAVING
            count(*) > 1
    ),

    /*Calculate the proportion of each project in each Boundary that it overlaps with*/
    aggregated_boundaries_nta_2 AS (
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
                            ST_Area(ST_Intersection(a.geometry, a.nta_geom))
                            / ST_Area(a.geometry)
                        )::decimal
                ELSE
                    1
            END AS proportion_in_nta
        FROM
            aggregated_boundaries_nta AS a
    ),

    /*
    If <10% of a project falls into a particular Boundary, then the sum of all proportions of a project in each Boundary would be <100%, because
    projects with less than 10% in a Boundary are not assigned to that Boundary. The next two steps ensure that 100% of each project's units are
    allocated to a Boundary.
    */
    aggregated_boundaries_nta_3 AS (
        SELECT
            source,
            record_id,
            sum(proportion_in_nta) AS total_proportion
        FROM
            aggregated_boundaries_nta_2
        GROUP BY
            source,
            record_id
    ),

    aggregated_boundaries_nta_4 AS (
        SELECT
            a.*,
            CASE
                WHEN
                    b.total_proportion IS NOT NULL
                    THEN (a.proportion_in_nta / b.total_proportion)::decimal
                ELSE 1
            END AS proportion_in_nta_1,
            CASE
                WHEN
                    b.total_proportion IS NOT NULL
                    THEN
                        round(
                            a.units_net
                            * (
                                a.proportion_in_nta
                                / b.total_proportion
                            )::decimal
                        )
                ELSE a.units_net
            END AS counted_units_1
        FROM
            aggregated_boundaries_nta_2 AS a
        LEFT JOIN
            aggregated_boundaries_nta_3 AS b
            ON
                a.record_id = b.record_id AND a.source = b.source
    )

    SELECT * FROM aggregated_boundaries_nta_4
) AS _1;
CREATE INDEX aggregated_nta_gix ON aggregated_nta USING gist (geometry gist_geometry_ops_2d);


/*Identify projects which did not geocode to any Boundary*/
SELECT *
INTO
ungeocoded_projects_nta
FROM (
    WITH ungeocoded_projects_nta AS (
        SELECT
            a.*,
            coalesce(a.nta, b.nta2020) AS nta_1,
            coalesce(
                a.nta_distance,
                ST_Distance(
                    nta_geom::geography,
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
            ) AS nta_distance1
        FROM
            aggregated_nta AS a
        LEFT JOIN
            dcp_nta2020 AS b
            ON
                a.nta_distance IS NULL
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
                                a.geometry::geography, nta_geom::geography, 500
                            )
                    WHEN ST_Area(a.geometry) > 0
                        THEN
                            ST_DWithin(
                                ST_Centroid(a.geometry)::geography,
                                nta_geom::geography,
                                500
                            )
                    ELSE
                        ST_DWithin(
                            a.geometry::geography, nta_geom::geography, 500
                        )
                END
    )

    SELECT * FROM ungeocoded_projects_nta
) AS _2;

/*Assign ungeocoded projects to their closest Boundary*/

SELECT *
INTO
aggregated_nta_longform
FROM (
    WITH min_distances AS (
        SELECT
            record_id,
            min(nta_distance1) AS min_distance
        FROM
            ungeocoded_projects_nta
        GROUP BY
            record_id
    ),

    all_projects_nta AS (
        SELECT a.*
        FROM
            ungeocoded_projects_nta AS a
        INNER JOIN
            min_distances AS b
            ON
                a.record_id = b.record_id
                AND a.nta_distance1 = b.min_distance
    )

    SELECT
        a.*,
        b.nta_1 AS nta,
        b.proportion_in_nta_1 AS proportion_in_nta,
        round(a.units_net * b.proportion_in_nta_1) AS units_net_in_nta,
        round(a.future_phased_units_total * b.proportion_in_nta_1) AS future_phased_units_total_in_nta,
        round(a.future_units_without_phasing * b.proportion_in_nta_1) AS future_units_without_phasing_in_nta,
        round(a.completed_units * b.proportion_in_nta_1) AS completed_units_in_nta,
        round(b.proportion_in_nta_1 * a.within_5_years::decimal
        ) AS within_5_years_in_nta,
        round(b.proportion_in_nta_1 * a.from_5_to_10_years::decimal
        ) AS from_5_to_10_years_in_nta,
        round(b.proportion_in_nta_1 * a.after_10_years::decimal
        ) AS after_10_years_in_nta
    FROM
        -- capitalplanning.kpdb_2021_09_10_nonull a 
        kpdb_deduplicated AS a
    LEFT JOIN
        all_projects_nta AS b
        ON
            a.source = b.source
            AND a.record_id = b.record_id
    ORDER BY
        source ASC,
        record_id ASC,
        record_name ASC,
        status ASC,
        b.nta_1 ASC
) AS _3
ORDER BY nta ASC;

/*Aggregate all results to the project-level, because if a project matches with multiple Boundarys, it'll appear in multiple rows*/

SELECT *
INTO
aggregated_nta_project_level
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
        has_project_phasing,
        has_future_units,
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
                        nullif(concat(nta), ''),
                        concat(round(100 * proportion_in_nta, 0), '%')
                    ),
                    ''
                )
            ),
            ' | '
        ) AS nta
    --geometry_webmercator 
    FROM
        (
            SELECT * FROM aggregated_nta_longform
            ORDER BY nta ASC
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
        has_project_phasing,
        has_future_units,
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
UPDATE aggregated_nta_longform a
SET
    nta = b.nta2020,
    proportion_in_nta = 1,
    units_net_in_nta = a.units_net
FROM dcp_nta2020 AS b
WHERE
    a.nta IS NULL
    AND NOT ST_IsEmpty(a.geometry)
    AND ST_Intersects(a.geometry, b.geometry);

/*
	Output final Boundary-based KPDB. This is not at the project-level, but rather the project & Boundary-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.


  	EP update 2021 - we now include completed DOB jobs in KPDB

*/

SELECT *
INTO
longform_nta_output
FROM (
    SELECT * FROM aggregated_nta_longform
    -- where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
    ORDER BY
        source ASC,
        record_id ASC,
        record_name ASC,
        status ASC
) AS x;


-- Drop intermediate tables
DROP TABLE IF EXISTS aggregated_nta;
DROP TABLE IF EXISTS ungeocoded_projects_nta;
DROP TABLE IF EXISTS aggregated_nta_longform;
DROP TABLE IF EXISTS aggregated_nta_project_level;

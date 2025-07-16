/*******************************************************************************************************************************************
Sources kpdb - Finalized version of KPDB build
		 doe_eszones

OUTPUT: longform_es_zone_output
*******************************************************************************************************************************************/

DROP TABLE IF EXISTS aggregated_es_zone;
DROP TABLE IF EXISTS ungeocoded_projects_es_zone;
DROP TABLE IF EXISTS aggregated_es_zone_longform;
DROP TABLE IF EXISTS aggregated_es_zone_project_level;
DROP TABLE IF EXISTS longform_es_zone_output;


SELECT *
INTO
aggregated_es_zone
FROM
    (
        WITH aggregated_boundaries_es_zone AS (
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
                b.geometry AS es_zone_geom,
                b.dbn AS es_zone,
                b.remarks AS es_remarks,
                ST_Distance(
                    a.geometry::geography, b.geometry::geography
                ) AS es_zone_distance
            FROM
                kpdb AS a
            LEFT JOIN
                doe_eszones AS b
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
                        WHEN ST_Area(a.geometry) > 0
                            THEN
                                ST_Intersects(
                                    ST_Centroid(a.geometry), b.geometry
                                )

                        /*Treating points as points*/
                        ELSE
                            ST_Intersects(a.geometry, b.geometry)
                    END
        /*Only matching if at least 10% of the polygon is in the boundary. Otherwise, the polygon will be apportioned to its other boundaries only*/
        ),

        /*Identify projects geocoded to multiple ESZs*/
        multi_geocoded_projects AS (
            SELECT
                source,
                record_id
            FROM
                aggregated_boundaries_es_zone
            GROUP BY
                source,
                record_id
            HAVING
                count(*) > 1
        ),

        /*Calculate the proportion of each project in each ES Zone that it overlaps with*/
        aggregated_boundaries_es_zone_2 AS (
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
                            (ST_Area(
                                ST_Intersection(a.geometry, a.es_zone_geom)
                            )
                            / ST_Area(a.geometry))::decimal
                    ELSE
                        1
                END AS proportion_in_es_zone
            FROM
                aggregated_boundaries_es_zone AS a
        ),

        /*
        If <10% of a project falls into a particular ES Zone, then the sum of all proportions of a project in each zone would be <100%, because
        projects with less than 10% in a zone are not assigned to that zone. The next two steps ensure that 100% of each project's units are
        allocated to a zone.
        */

        aggregated_boundaries_es_zone_3 AS (
            SELECT
                source,
                record_id,
                sum(proportion_in_es_zone) AS total_proportion
            FROM
                aggregated_boundaries_es_zone_2
            GROUP BY
                source,
                record_id
        ),

        aggregated_boundaries_es_zone_4 AS (
            SELECT
                a.*,
                CASE
                    WHEN
                        b.total_proportion IS NOT NULL
                        THEN
                            (
                                a.proportion_in_es_zone
                                / b.total_proportion
                            )::decimal
                    ELSE 1
                END AS proportion_in_es_zone_1,
                CASE
                    WHEN
                        b.total_proportion IS NOT NULL
                        THEN
                            round(
                                a.units_net
                                * (
                                    a.proportion_in_es_zone
                                    / b.total_proportion
                                )::decimal
                            )
                    ELSE a.units_net
                END AS units_net_1
            FROM
                aggregated_boundaries_es_zone_2 AS a
            LEFT JOIN
                aggregated_boundaries_es_zone_3 AS b
                ON
                    a.record_id = b.record_id AND a.source = b.source
        )

        SELECT * FROM aggregated_boundaries_es_zone_4

    ) AS _1;


/*Identify projects which did not geocode to any ES Zone*/
SELECT *
INTO
ungeocoded_projects_es_zone
FROM
    (
        WITH ungeocoded_projects_es_zone AS (
            SELECT
                a.*,
                coalesce(a.es_zone, b.dbn) AS es_zone_1,
                coalesce(a.es_remarks, b.remarks) AS es_remarks_1,
                coalesce(
                    a.es_zone_distance,
                    ST_Distance(
                        b.geometry::geography,
                        CASE
                            WHEN
                                (
                                    ST_Area(a.geometry::geography) > 10000
                                    OR units_gross > 500
                                )
                                AND a.source IN (
                                    'DCP Application',
                                    'DCP Planner-Added PROJECTs'
                                )
                                THEN a.geometry::geography
                            WHEN
                                ST_Area(a.geometry) > 0
                                THEN ST_Centroid(a.geometry)::geography
                            ELSE a.geometry::geography
                        END
                    )
                ) AS es_zone_distance1
            FROM
                aggregated_es_zone AS a
            LEFT JOIN
                doe_eszones AS b
                ON
                    a.es_zone_distance IS NULL
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
                                    a.geometry::geography,
                                    b.geometry::geography,
                                    500
                                )
                        WHEN ST_Area(a.geometry) > 0
                            THEN
                                ST_DWithin(
                                    ST_Centroid(a.geometry)::geography,
                                    b.geometry::geography,
                                    500
                                )
                        ELSE
                            ST_DWithin(
                                a.geometry::geography,
                                b.geometry::geography,
                                500
                            )
                    END
        )

        SELECT * FROM ungeocoded_projects_es_zone
    ) AS _2;


SELECT *
INTO
aggregated_es_zone_longform
FROM
    (
        WITH min_distances AS (
            SELECT
                record_id,
                min(es_zone_distance1) AS min_distance
            FROM
                ungeocoded_projects_es_zone
            GROUP BY
                record_id
        ),

        all_projects_es_zone AS (
            SELECT a.*
            FROM
                ungeocoded_projects_es_zone AS a
            INNER JOIN
                min_distances AS b
                ON
                    a.record_id = b.record_id
                    AND a.es_zone_distance1 = b.min_distance
        )

        SELECT
            a.*,
            b.es_zone_1 AS es_zone,
            b.es_remarks_1 AS es_remarks,
            b.proportion_in_es_zone_1 AS proportion_in_es_zone,
            coalesce(
                b.es_zone_1,
                CASE
                    WHEN
                        b.es_remarks_1 LIKE '%Contact %'
                        THEN
                            substring(
                                b.es_remarks_1,
                                1,
                                position('Contact' IN b.es_remarks_1) - 1
                            )
                    ELSE b.es_remarks_1
                END
            ) AS es_zone_remarks,
            round(
                a.units_net * b.proportion_in_es_zone_1
            ) AS units_net_in_es_zone
        FROM
            kpdb AS a
        LEFT JOIN
            all_projects_es_zone AS b
            ON
                a.source = b.source
                AND a.record_id = b.record_id
        ORDER BY
            source ASC,
            record_id ASC,
            record_name ASC,
            status ASC,
            b.es_zone_1 ASC
    ) AS _3
ORDER BY
    es_zone ASC;


SELECT *
INTO
aggregated_es_zone_project_level
FROM
    (
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
                            nullif(
                                es_zone_remarks,
                                ''
                            ),
                            concat(round(100 * proportion_in_es_zone, 0), '%')
                        ),
                        ''
                    )
                ),
                ' | '
            ) AS es_zone
        --geometry_webmercator
        FROM
            aggregated_es_zone_longform
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

/*
	Output final ES-zone-based KPDB. This is not at the project-level, but rather the project & ES-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.

  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations
*/

SELECT *
INTO
longform_es_zone_output
FROM
    (
        SELECT * FROM aggregated_es_zone_longform
        --where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
        ORDER BY
            source ASC,
            record_id ASC,
            record_name ASC,
            status ASC
    ) AS x;

-- Drop intermediate tables
DROP TABLE IF EXISTS aggregated_es_zone;
DROP TABLE IF EXISTS ungeocoded_projects_es_zone;
DROP TABLE IF EXISTS aggregated_es_zone_longform;
DROP TABLE IF EXISTS aggregated_es_zone_project_level;

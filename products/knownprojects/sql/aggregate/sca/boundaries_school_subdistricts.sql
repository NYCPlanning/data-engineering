/**********************************************************************************************************************************************************************************
Sources kpdb - finalized version of KPDB build
		 doe_schoolsubdistricts
OUTPUT: longform_subdist_output_cp_assumptions

*******************************************************************************************************************************************/

DROP TABLE IF EXISTS aggregated_subdist_cp_assumptions;
DROP TABLE IF EXISTS ungeocoded_projects_subdist_cp_assumptions;
DROP TABLE IF EXISTS aggregated_subdist_longform_cp_assumptions;
DROP TABLE IF EXISTS aggregated_subdist_project_level_cp_assumptions;
DROP TABLE IF EXISTS longform_subdist_output_cp_assumptions;

SELECT *
INTO
aggregated_subdist_cp_assumptions
FROM (
    WITH aggregated_boundaries_subdist AS (
        SELECT
            --	a.cartodb_id,
            a.geometry,
            --	a.geometry_webmercator,
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
            b.geometry AS subdist_geom,
            b.district AS distzone,
            b.subdistrict AS subdistzone,
            b.name AS a_dist_zone_name,
            ST_Distance(
                a.geometry::geography, b.geometry::geography
            ) AS subdiST_Distance
        FROM
            kpdb AS a
        LEFT JOIN
            doe_school_subdistricts AS b
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
                            'DCP Application', 'DCP Planner-Added Projects'
                        )
                        THEN
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
                            'DCP Application', 'DCP Planner-Added Projects'
                        )
                        THEN
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
                            ST_Intersects(a.geometry, b.geometry)
                            AND (ST_Area(
                                ST_Intersection(a.geometry, b.geometry)
                            )
                            / ST_Area(a.geometry))::decimal
                            >= .1

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

    multi_geocoded_projects AS (
        SELECT
            source,
            record_id
        FROM
            aggregated_boundaries_subdist
        GROUP BY
            source,
            record_id
        HAVING
            count(*) > 1
    ),

    aggregated_boundaries_subdist_2 AS (
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
                            ST_Area(ST_Intersection(a.geometry, a.subdist_geom))
                            / ST_Area(a.geometry)
                        )::decimal
                ELSE
                    1
            END AS proportion_in_subdist
        FROM
            aggregated_boundaries_subdist AS a
    ),

    aggregated_boundaries_subdist_3 AS (
        SELECT
            source,
            record_id,
            sum(proportion_in_subdist) AS total_proportion
        FROM
            aggregated_boundaries_subdist_2
        GROUP BY
            source,
            record_id
    ),

    aggregated_boundaries_subdist_4 AS (
        SELECT
            a.*,
            CASE
                WHEN
                    b.total_proportion IS NOT NULL
                    THEN (a.proportion_in_subdist / b.total_proportion)::decimal
                ELSE 1
            END AS proportion_in_subdist_1,
            CASE
                WHEN
                    b.total_proportion IS NOT NULL
                    THEN
                        round(
                            a.units_net
                            * (
                                a.proportion_in_subdist
                                / b.total_proportion
                            )::decimal
                        )
                ELSE a.units_net
            END AS units_net_1
        FROM
            aggregated_boundaries_subdist_2 AS a
        LEFT JOIN
            aggregated_boundaries_subdist_3 AS b
            ON
                a.record_id = b.record_id AND a.source = b.source
    )

    SELECT * FROM aggregated_boundaries_subdist_4

) AS _1;


/*Identify projects which did not geocode to any Subdistrict*/

SELECT *
INTO
ungeocoded_projects_subdist_cp_assumptions
FROM (
    WITH ungeocoded_projects_subdist AS (
        SELECT
            a.*,
            coalesce(a.distzone, b.district) AS distzone_1,
            coalesce(a.subdistzone, b.subdistrict) AS subdistzone_1,
            coalesce(a.a_dist_zone_name, b.name) AS a_dist_zone_name_1,
            coalesce(
                a.subdiST_Distance,
                ST_Distance(
                    b.geometry::geography,
                    CASE
                        WHEN
                            (
                                ST_Area(a.geometry::geography) > 10000
                                OR units_gross > 500
                            )
                            AND a.source IN (
                                'DCP Application', 'DCP Planner-Added Projects'
                            )
                            THEN a.geometry::geography
                        WHEN
                            ST_Area(a.geometry) > 0
                            THEN ST_Centroid(a.geometry)::geography
                        ELSE a.geometry::geography
                    END
                )
            ) AS subdiST_Distance1
        FROM
            aggregated_subdist_cp_assumptions AS a
        LEFT JOIN
            doe_school_subdistricts AS b
            ON
                a.subdiST_Distance IS NULL
                AND CASE
                    WHEN
                        (
                            ST_Area(a.geometry::geography) > 10000
                            OR units_gross > 500
                        )
                        AND a.source IN (
                            'DCP Application', 'DCP Planner-Added Projects'
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
                            a.geometry::geography, b.geometry::geography, 500
                        )
                END
    )

    SELECT * FROM ungeocoded_projects_subdist
) AS _2;


DROP TABLE IF EXISTS aggregated_subdist_longform_cp_assumptions;

SELECT *
INTO
aggregated_subdist_longform_cp_assumptions
FROM (
    WITH min_distances AS (
        SELECT
            record_id,
            min(subdiST_Distance1) AS min_distance
        FROM
            ungeocoded_projects_subdist_cp_assumptions
        GROUP BY
            record_id
    ),

    all_projects_subdist AS (
        SELECT a.*
        FROM
            ungeocoded_projects_subdist_cp_assumptions AS a
        INNER JOIN
            min_distances AS b
            ON
                a.record_id = b.record_id
                AND a.subdiST_Distance1 = b.min_distance
    )

    SELECT
        a.*,
        b.distzone_1 AS distzone,
        b.subdistzone_1 AS subdistzone,
        b.a_dist_zone_name_1 AS a_dist_zone_name,
        b.proportion_in_subdist_1 AS proportion_in_subdist,
        round(a.units_net * b.proportion_in_subdist_1) AS units_net_in_subdist
    FROM
        kpdb AS a
    LEFT JOIN
        all_projects_subdist AS b
        ON
            a.source = b.source
            AND a.record_id = b.record_id
    ORDER BY
        source ASC,
        record_id ASC,
        record_name ASC,
        status ASC,
        b.distzone_1 ASC,
        b.subdistzone_1 ASC
) AS _3
ORDER BY distzone ASC;


SELECT *
INTO
aggregated_subdist_project_level_cp_assumptions
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
                        nullif(distzone, ''),
                        concat(round(100 * proportion_in_subdist, 0), '%')
                    ),
                    ''
                )
            ),
            ' | '
        ) AS distzone,
        array_to_string(
            array_agg(
                nullif(
                    concat_ws(
                        ': ',
                        nullif(a_dist_zone_name, ''),
                        concat(round(100 * proportion_in_subdist, 0), '%')
                    ),
                    ''
                )
            ),
            ' | '
        ) AS a_dist_zone_name
    --geometry_webmercator
    FROM
        aggregated_subdist_longform_cp_assumptions
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
	Output final subdistrict KPDB. This is not at the project-level, but rather the project & subdist-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.


  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations

*/

-- this is a bit fragile - if remaining unassigned projects overlapped with multiple, this would have undesired behavior
-- quick fix on 3/29/23 to fix 43 records not matching. Verified via manual querying that this has desired outcome
-- currently not fixing any records, so commenting out
/*
UPDATE aggregated_subdist_longform_cp_assumptions a
    SET
        distzone = b.district,
		subdistzone = b.subdistrict,
		a_dist_zone_name = b.name,
        proportion_in_csd = 1,
        units_net_in_csd = a.units_net,
FROM dcp_school_districts b
WHERE a.distzone IS NULL
    AND a.subdistzone IS NULL
    AND NOT ST_IsEmpty(a.geometry)
    AND ST_Intersects(a.geometry, b.geometry);
*/

SELECT *
INTO
longform_subdist_output_cp_assumptions
FROM (
    SELECT * FROM aggregated_subdist_longform_cp_assumptions
-- where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
) AS x;

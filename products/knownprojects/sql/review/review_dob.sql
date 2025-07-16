/*
DESCRIPTION:
    Identies dcp_housing records that spatially overlap with
    and are within a few years of non-DOB records.
INPUTS:
    combined
    dcp_housing_poly
OUTPUTS:
    _review_dob
*/
DROP TABLE IF EXISTS _review_dob;
WITH overlap_projects AS (
    -- Projects that contain overlaps
    SELECT
        project_record_ids,
        unnest(project_record_ids) AS record_id,
        row_number() OVER (
            ORDER BY project_record_ids
        ) AS project_id
    FROM _project_record_ids
),

stand_alone_projects AS (
    -- Stand-alone records
    SELECT
        record_id,
        ARRAY[]::text [] || record_id AS project_record_ids,
        row_number()
            OVER (
                ORDER BY record_id
            )
        + (SELECT max(project_id) FROM overlap_projects
        ) AS project_id
    FROM (
        SELECT record_id::text FROM combined
        WHERE
            source NOT IN (
                'DOB',
                'Neighborhood Study Rezoning Commitments',
                'Future Neighborhood Studies'
            )
    ) AS a
    WHERE
        record_id NOT IN (
            SELECT unnest(project_record_ids) FROM _project_record_ids
        )

),

b AS (
    SELECT
        project_id,
        record_id,
        project_record_ids
    FROM overlap_projects
    UNION
    SELECT
        project_id,
        record_id,
        project_record_ids
    FROM stand_alone_projects
),

projects AS (
    SELECT
        a.*,
        b.project_record_ids,
        b.project_id
    FROM combined AS a
    INNER JOIN b ON a.record_id = b.record_id
),

matches AS (
    /*
    Identify records that intersect with DOB jobs. This excludes records from EDC
    Projected Projects, and has a time constraint.
    */
    SELECT
        b.*,
        a.record_id AS match_record_id,
        a.record_id_input AS match_record_id_input,
        a.project_id,
        a.project_record_ids || b.record_id AS project_record_ids
    FROM projects AS a
    INNER JOIN combined AS b
        ON
            ST_Intersects(a.geom, b.geom)
            AND ST_GeometryType(ST_Intersection(a.geom, b.geom)) = 'ST_Polygon'
            AND (CASE
                -- EDC Projected Projects match with DOB records of any date
                WHEN a.source = 'EDC Projected Projects' THEN TRUE
                -- Only include DOB jobs permitted after, or within 2 years prior, to non-DOB date 
                ELSE
                    (CASE
                        -- Include non-permitted DOB records
                        WHEN b.date IS NULL THEN TRUE
                        -- Compare permitted DOB records to non-DOB date, where available, or current date
                        ELSE (CASE
                            WHEN a.date IS NOT NULL
                                THEN
                                    extract(YEAR FROM b.date::timestamp)
                                    >= split_part(
                                        split_part(a.date, '/', 1), '-', 1
                                    )::numeric
                                    - 2
                            ELSE
                                extract(YEAR FROM b.date::timestamp)
                                >= extract(YEAR FROM current_date) - 2
                        END)
                    END)
            END)
    WHERE
        b.source = 'DOB'
        AND a.geom IS NOT NULL
        AND b.geom IS NOT NULL
),

combined_dob AS (
    -- Combine matched DOB records with records from project table 
    SELECT
        source,
        record_id,
        record_name,
        status,
        type,
        units_gross,
        date,
        date_type,
        inactive,
        no_classa,
        project_record_ids,
        project_id,
        geom
    FROM matches
    UNION
    SELECT
        source,
        record_id,
        record_name,
        status,
        type,
        units_gross,
        date,
        date_type,
        inactive,
        no_classa,
        project_record_ids,
        project_id,
        geom
    FROM projects
),

multimatch AS (
    SELECT record_id FROM matches
    GROUP BY record_id
    HAVING count(DISTINCT project_id) > 1
),

multimatchproject AS (
    SELECT project_id
    FROM matches
    WHERE record_id IN (SELECT record_id FROM multimatch)
)

SELECT
    a.source,
    a.record_id,
    a.record_name,
    b.job_desc,
    a.status,
    a.type,
    a.units_gross,
    a.date,
    a.date_type,
    a.inactive,
    a.no_classa,
    a.project_record_ids,
    b.classa_init,
    b.classa_prop,
    b.otherb_init,
    b.otherb_prop,
    b.date_filed,
    b.date_lastupdt,
    b.date_complete,
    (
        a.record_id IN (SELECT record_id FROM multimatch) AND a.source = 'DOB'
    )::integer AS dob_multimatch,
    (
        a.project_id IN (SELECT project_id FROM multimatchproject)
    )::integer AS project_has_dob_multi,
    (a.geom IS NULL)::integer AS no_geom,
    ST_CollectionExtract(a.geom, 3) AS geom,
    now() AS v
INTO _review_dob
FROM combined_dob AS a
LEFT JOIN dcp_housing_poly AS b
    ON a.record_id = b.record_id
-- Only output matched DOB jobs and the records associated with them for review
WHERE
    a.record_id IN (
        SELECT record_id FROM matches
        UNION
        SELECT unnest(project_record_ids) FROM matches
    )
ORDER BY array_to_string(project_record_ids, ',');

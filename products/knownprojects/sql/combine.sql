/*
DESCRIPTION:
    Combines input data into a single table with a shared schema. For several
    data sources, this involves joining on BBL with dcp_mappluto_wi to get lot-level
    polygon geometry. For some input datasets, a rows get collapsed to the level of
    a project. In these cases, the unique IDs for input data rows get stored in
    the field record_id_input as an array. If no collapsing was necessary (i.e.
    each project a single record in the a given source data table), the record_id_input
    array only contains the record_id. In cases where there is no unique project-level
    ID, the record_id gets assigned from a hash of the uids in record_id_input.
    This script also includes standardization of statuses, initial phasing assumptions,
    and calls to various string parsing functions to set flags.
INPUTS:
    dcp_mappluto_wi
    dcp_application
    edc_projects
    edc_dcp_inputs
    dcp_planner_added
    dcp_n_study_future
    dcp_rezoning
    dcp_n_study_projected
    dcp_n_study
    esd_projects
    hpd_pc
    hpd_rfp
    dcp_housing_poly
OUTPUTS:
    combined
*/
DROP TABLE IF EXISTS combined;
WITH
_dcp_application AS (
    SELECT
        source,
        record_id,
        record_name,
        status,
        NULL AS type,
        units_gross,
        dcp_certifiedreferred AS date,
        'Certified Referred' AS date_type,
        0 AS prop_within_5_years,
        0 AS prop_after_10_years,
        0 AS phasing_known,
        geom,
        array_append(ARRAY[]::text [], a.record_id::text) AS record_id_input,
        (CASE
            WHEN status = 'Record Closed'
                THEN 0
            ELSE 1
        END) AS prop_5_to_10_years,
        flag_nycha(a::text) AS nycha,
        flag_classb(a::text) AS classb,
        flag_senior_housing(a::text) AS senior_housing
    FROM dcp_application AS a
    WHERE flag_relevant = 1
),

_edc_projects AS (
    WITH
    geom_bbl AS (
        SELECT
            a.uid,
            st_union(b.wkb_geometry) AS geom
        FROM (
            SELECT
                uid,
                unnest(string_to_array(coalesce(bbl, 'NA'), ';')) AS bbl
            FROM edc_projects
        ) AS a LEFT JOIN dcp_mappluto_wi AS b
            ON a.bbl = b.bbl::bigint::text
        GROUP BY a.uid
    ),

    geom_borough_block AS (
        SELECT
            a.uid,
            st_union(b.wkb_geometry) AS geom
        FROM edc_projects AS a
        LEFT JOIN dcp_mappluto_wi AS b
            ON
                a.block = b.block::text
                AND a.borough_code = b.borocode::text
        GROUP BY a.uid
    ),

    geom_edc_dcp_inputs AS (
        SELECT
            a.uid,
            b.geometry AS geom
        FROM edc_projects AS a
        LEFT JOIN edc_dcp_inputs AS b
            ON a.edc_id::numeric = b.edc_id::numeric
    ),

    geom_consolidated AS (
        SELECT
            a.uid,
            coalesce(a.geom, bb.geom) AS geom
        FROM (
            SELECT
                a.uid,
                coalesce(a.geom, b.geom) AS geom
            FROM geom_edc_dcp_inputs AS a LEFT JOIN geom_bbl AS b
                ON a.uid = b.uid
        ) AS a LEFT JOIN geom_borough_block AS bb
            ON a.uid = bb.uid
    )

    SELECT
        'EDC Projected Projects' AS source,
        a.uid AS record_id,
        project_name AS record_name,
        'Potential' AS status,
        NULL AS type,
        total_units::numeric AS units_gross,
        build_year AS date,
        'Build Year' AS date_type,
        1 AS phasing_known,

        -- phasing
        b.geom,
        array_append(ARRAY[]::text [], a.uid) AS record_id_input,
        (CASE
            WHEN build_year::numeric <= date_part('year', current_date) + 5
                THEN 1
            ELSE 0
        END) AS prop_within_5_years,
        (CASE
            WHEN
                build_year::numeric > date_part('year', current_date) + 5
                AND build_year::numeric <= date_part('year', current_date) + 10
                THEN 1
            ELSE 0
        END) AS prop_5_to_10_years,
        (CASE
            WHEN build_year::numeric > date_part('year', current_date) + 10
                THEN 1
            ELSE 0
        END) AS prop_after_10_years,
        flag_nycha(a::text) AS nycha,
        flag_classb(a::text) AS classb,
        flag_senior_housing(a::text) AS senior_housing
    FROM edc_projects AS a
    LEFT JOIN geom_consolidated AS b
        ON a.uid = b.uid
),

_dcp_planneradded AS (
    SELECT
        'DCP Planner-Added Projects' AS source,
        uid AS record_id,
        project_na AS record_name,
        NULL AS status,
        NULL AS type,
        total_unit::numeric AS units_gross,
        NULL AS date,
        NULL AS date_type,
        portion_bu::numeric AS prop_within_5_years,
        portion_1::numeric AS prop_5_to_10_years,
        portion_2::numeric AS prop_after_10_years,
        1 AS phasing_known,
        geometry AS geom,
        array_append(ARRAY[]::text [], a.uid) AS record_id_input,
        flag_nycha(a::text) AS nycha,
        flag_classb(a::text) AS classb,
        flag_senior_housing(a::text) AS senior_housing
    FROM dcp_planneradded AS a
),

_dcp_n_study_future AS (
    SELECT
        'Future Neighborhood Studies' AS source,
        uid AS record_id,
        'Potential' AS status,
        'Future Rezoning' AS type,
        total_unit::numeric AS units_gross,
        adoption AS date,
        'Effective Date' AS date_type,
        within5::numeric AS prop_within_5_years,
        "5to10"::numeric AS prop_5_to_10_years,
        after10::numeric AS prop_after_10_years,
        0 AS phasing_known,
        geometry AS geom,
        NULL::integer AS nycha,
        NULL::integer AS classb,
        NULL::integer AS senior_housing,
        array_append(ARRAY[]::text [], uid) AS record_id_input,
        project_id || ' ' || 'Future Rezoning Development' AS record_name
    FROM dcp_n_study_future
),

_dcp_n_study_projected AS (
    SELECT
        'Neighborhood Study Projected Development Sites' AS source,
        uid AS record_id,
        'Potential' AS status,
        'Past Rezoning' AS type,
        total_unit::numeric AS units_gross,
        adoption AS date,
        'Effective Date' AS date_type,
        -- TO_CHAR(TO_DATE(effective_date, 'MM/DD/YYYY'), 'YYYY/MM/DD') as date,
        within5::numeric AS prop_within_5_years,
        "5to10"::numeric AS prop_5_to_10_years,
        after10::numeric AS prop_after_10_years,
        1 AS phasing_known,
        geometry AS geom,
        NULL::integer AS nycha,
        NULL::integer AS classb,
        NULL::integer AS senior_housing,
        array_append(ARRAY[]::text [], a.uid) AS record_id_input,
        replace(study, ' Projected Development Sites', '') AS record_name
    FROM dcp_n_study_projected AS a
),

_dcp_n_study AS (
    SELECT
        source,
        uid::text AS record_id,
        project_na AS record_name,
        'Potential' AS status,
        NULL::text AS type,
        total_unit::numeric AS units_gross,
        NULL::text AS date,
        NULL::text AS date_type,
        within5::numeric AS prop_within_5_years,
        "5to10"::numeric AS prop_5_to_10_years,
        after10::numeric AS prop_after_10_years,
        1 AS phasing_known,
        geometry AS geom,
        NULL::integer AS nycha,
        NULL::integer AS classb,
        NULL::integer AS senior_housing,
        array_append(ARRAY[]::text [], uid::text) AS record_id_input
    FROM dcp_n_study
),

_esd_projects AS (
    SELECT
        'Empire State Development Projected Projects' AS source,
        a.project_name AS record_name,
        'Potential' AS status,
        NULL AS type,
        total_units::numeric AS units_gross,
        NULL AS date,
        NULL AS date_type,
        NULL::numeric AS prop_within_5_years,
        NULL::numeric AS prop_5_to_10_years,
        1 AS prop_after_10_years,
        0 AS phasing_known,
        md5(array_to_string(array_agg(a.uid), '')) AS record_id,
        array_agg(a.uid) AS record_id_input,
        st_union(b.wkb_geometry) AS geom,
        flag_nycha(array_agg(row_to_json(a))::text) AS nycha,
        flag_classb(array_agg(row_to_json(a))::text) AS classb,
        flag_senior_housing(array_agg(row_to_json(a))::text) AS senior_housing
    FROM esd_projects AS a
    LEFT JOIN dcp_mappluto_wi AS b
        ON a.bbl::numeric = b.bbl::numeric
    GROUP BY project_name, total_units
),

_hpd_pc AS (
    SELECT
        'HPD Projected Closings' AS source,
        a.uid AS record_id,
        'HPD 3: Projected Closing' AS status,
        NULL AS type,
        ((
            min_of_projected_units::integer
            + max_of_projected_units::integer
        ) / 2
        )::integer AS units_gross,
        projected_fiscal_year_range AS date,
        'Projected Fiscal Year Range' AS date_type,

        -- dates
        (CASE
            WHEN
                date_part(
                    'year',
                    age(
                        to_date((concat(right(projected_fiscal_year_range, 4)::numeric + 3, '-06-30')), 'YYYY-MM-DD'),
                        current_date
                    )
                )
                <= 5
                THEN 1
            ELSE 0
        END)::numeric AS prop_within_5_years,
        (CASE
            WHEN
                date_part(
                    'year',
                    age(
                        to_date((concat(right(projected_fiscal_year_range, 4)::numeric + 3, '-06-30')), 'YYYY-MM-DD'),
                        current_date
                    )
                )
                > 5
                AND date_part(
                    'year',
                    age(
                        to_date((concat(right(projected_fiscal_year_range, 4)::numeric + 3, '-06-30')), 'YYYY-MM-DD'),
                        current_date
                    )
                )
                <= 10
                THEN 1
            ELSE 0
        END)::numeric AS prop_5_to_10_years,

        -- phasing
        (CASE
            WHEN
                date_part(
                    'year',
                    age(
                        to_date((concat(right(projected_fiscal_year_range, 4)::numeric + 3, '-06-30')), 'YYYY-MM-DD'),
                        current_date
                    )
                )
                > 10
                THEN 1
            ELSE 0
        END)::numeric AS prop_after_10_years,

        1 AS phasing_known,

        b.wkb_geometry AS geom,
        array_append(ARRAY[]::text [], a.uid) AS record_id_input,
        house_number || ' ' || street_name AS record_name,
        flag_nycha(a::text) AS nycha,
        flag_classb(a::text) AS classb,
        flag_senior_housing(a::text) AS senior_housing
    FROM hpd_pc AS a
    LEFT JOIN dcp_mappluto_wi AS b
        ON a.bbl::numeric = b.bbl::numeric
),

_hpd_rfp AS (
    SELECT
        'HPD RFPs' AS source,
        request_for_proposals_name AS record_name,
        NULL AS type,
        (CASE
            WHEN est_units ~* '-' THEN NULL
            ELSE replace(est_units, ',', '')
        END)::integer AS units_gross,
        'Month Closed' AS date_type,
        1 AS prop_within_5_years,
        0 AS prop_5_to_10_years,
        0 AS prop_after_10_years,
        1 AS phasing_known,
        md5(array_to_string(array_agg(a.uid), '')) AS record_id,
        array_agg(a.uid) AS record_id_input,
        (CASE
            WHEN designated = 'Y' AND closed = 'Y'
                THEN 'HPD 4: Financing Closed'
            WHEN designated = 'Y' AND closed = 'N'
                THEN 'HPD 2: RFP Designated'
            WHEN designated = 'N' AND closed = 'N'
                THEN 'HPD 1: RFP Issued'
        END) AS status,
        (CASE
            WHEN closed_date = '-' THEN NULL
            ELSE to_char(closed_date::date, 'YYYY/MM')
        END) AS date,
        st_union(b.wkb_geometry) AS geom,
        flag_nycha(array_agg(row_to_json(a))::text) AS nycha,
        flag_classb(array_agg(row_to_json(a))::text) AS classb,
        flag_senior_housing(array_agg(row_to_json(a))::text) AS senior_housing
    FROM hpd_rfp AS a
    LEFT JOIN dcp_mappluto_wi AS b
        ON a.bbl::numeric = b.bbl::numeric
    GROUP BY
        request_for_proposals_name, designated,
        closed, est_units, closed_date, likely_to_be_built_by_2025
),

/* Housing data, as mapped in _sql/dcp_housing.sql
    NOTE: this still includes contextual class B records */
_dcp_housing AS (
    SELECT
        source,
        record_id::text,
        record_name,
        status,
        type,
        units_gross::integer,
        date,
        date_type,
        prop_within_5_years,
        prop_5_to_10_years,
        prop_after_10_years,
        phasing_known,
        geom,
        nycha,
        classb,
        senior_housing,
        inactive,
        no_classa,
        array_append(ARRAY[]::text [], record_id::text) AS record_id_input
    FROM dcp_housing_poly
)

SELECT
    *,
    NULL::text AS phasing_rationale
INTO combined
FROM (
    SELECT
        *,
        NULL::numeric AS inactive,
        NULL::text AS no_classa
    FROM (
        SELECT
            source,
            record_id,
            record_name,
            status,
            type,
            units_gross,
            date,
            date_type,
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _dcp_application
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _edc_projects
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _dcp_planneradded
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _dcp_n_study
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _dcp_n_study_future
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _dcp_n_study_projected
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _esd_projects
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _hpd_pc
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
            prop_within_5_years,
            prop_5_to_10_years,
            prop_after_10_years,
            phasing_known,
            nycha,
            classb,
            senior_housing,
            record_id_input,
            st_makevalid(geom) AS geom
        FROM _hpd_rfp
    ) AS a
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
        prop_within_5_years,
        prop_5_to_10_years,
        prop_after_10_years,
        phasing_known,
        nycha,
        classb,
        senior_housing,
        record_id_input,
        st_makevalid(geom) AS geom,
        inactive,
        no_classa
    FROM _dcp_housing
) AS a
WHERE record_id NOT IN (
    SELECT record_id FROM corrections_main
    WHERE field = 'remove'
);

UPDATE combined
SET geom = CASE
    WHEN st_isempty(geom) THEN NULL
    WHEN geometrytype(geom) = 'GEOMETRYCOLLECTION' THEN st_makevalid(st_collectionextract(geom, 3))
    ELSE st_makevalid(geom)
END;

CREATE INDEX combined_geom_idx ON combined USING gist (geom);

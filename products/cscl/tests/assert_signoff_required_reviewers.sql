-- Reviewer statuses follow the signoff_matrix: a reviewer the matrix requires for an
-- output is never n_a, one it doesn't require always is. Every known_differences
-- `output` value belongs to exactly one matrix row, and every lion_outputs output_group
-- has a matrix row. Reads only seeds.

WITH kd_groups AS (
    {{ known_difference_signoff() }}
),

kd_unmapped AS (
    SELECT
        'known_differences' AS seed,
        id,
        'an output no signoff_matrix row lists' AS problem
    FROM kd_groups
    WHERE output_group IS NULL
),

matrix_output_counts AS (
    SELECT
        unnest(list_transform(string_split(known_difference_outputs, ';'), o -> trim(o)))
            AS output
    FROM {{ ref('signoff_matrix') }}
),

matrix_duplicates AS (
    SELECT
        'signoff_matrix' AS seed,
        output AS id,
        'output listed in more than one row' AS problem
    FROM matrix_output_counts
    GROUP BY output
    HAVING count(*) > 1
),

-- a difference spanning groups needs the union of their reviewers
kd_required AS (
    SELECT
        id,
        bool_or(requires_gr) AS requires_gr,
        bool_or(requires_gss) AS requires_gss,
        bool_or(requires_gis) AS requires_gis,
        any_value(gr_status) AS gr_status,
        any_value(gss_status) AS gss_status,
        any_value(gis_status) AS gis_status
    FROM kd_groups
    WHERE output_group IS NOT NULL
    GROUP BY id
),

file_required AS (
    SELECT
        outputs.file_id AS id,
        matrix.requires_gr,
        matrix.requires_gss,
        matrix.requires_gis,
        outputs.gr_signoff AS gr_status,
        outputs.gss_signoff AS gss_status,
        outputs.gis_signoff AS gis_status
    FROM {{ ref('lion_outputs') }} AS outputs
    LEFT JOIN {{ ref('signoff_matrix') }} AS matrix
        ON outputs.output_group = matrix.output_group
),

file_unmapped AS (
    SELECT
        'lion_outputs' AS seed,
        id,
        'output_group has no signoff_matrix row' AS problem
    FROM file_required
    WHERE requires_gr IS NULL
),

required AS (
    SELECT
        'known_differences' AS seed,
        *
    FROM kd_required
    UNION ALL
    SELECT
        'lion_outputs' AS seed,
        *
    FROM file_required
    WHERE requires_gr IS NOT NULL
),

by_reviewer AS (
    SELECT
        seed,
        id,
        'gr' AS reviewer,
        requires_gr AS is_required,
        gr_status AS reviewer_status
    FROM required
    UNION ALL
    SELECT
        seed,
        id,
        'gss' AS reviewer,
        requires_gss AS is_required,
        gss_status AS reviewer_status
    FROM required
    UNION ALL
    SELECT
        seed,
        id,
        'gis' AS reviewer,
        requires_gis AS is_required,
        gis_status AS reviewer_status
    FROM required
),

mismatched AS (
    SELECT
        seed,
        id,
        CASE
            WHEN is_required THEN reviewer || ' is required but n_a'
            ELSE reviewer || ' is not required, so must be n_a'
        END AS problem
    FROM by_reviewer
    WHERE (reviewer_status = 'n_a') = is_required
)

SELECT * FROM kd_unmapped
UNION ALL
SELECT * FROM matrix_duplicates
UNION ALL
SELECT * FROM file_unmapped
UNION ALL
SELECT * FROM mismatched

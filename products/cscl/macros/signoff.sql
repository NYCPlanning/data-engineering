{#
    Reviewer sign-off (see the signoff_matrix seed).

    known_difference_signoff(): one row per (known difference, output_group it touches),
    with the reviewers that output_group requires and the difference's status from each.
    A known difference's `output` values map to output groups through the matrix's
    known_difference_outputs column; an output value no matrix row lists gets a NULL
    output_group (assert_signoff_required_reviewers fails on it). Reads only seeds, so
    the seed tests can use it too.

    approved: every reviewer the output_group requires has agreed.
#}
{% macro known_difference_signoff() %}
    WITH kd_outputs AS (
        SELECT
            kd.id,
            unnest(list_transform(string_split(kd.output, ';'), o -> trim(o))) AS output,
            kd.gr_status,
            kd.gss_status,
            kd.gis_status,
            kd.reviewed_on,
            kd.review_ref
        FROM {{ ref('known_differences') }} AS kd
    ),

    matrix_outputs AS (
        SELECT
            output_group,
            unnest(list_transform(string_split(known_difference_outputs, ';'), o -> trim(o))) AS output,
            requires_gr,
            requires_gss,
            requires_gis
        FROM {{ ref('signoff_matrix') }}
    )

    SELECT DISTINCT
        kd_outputs.id,
        matrix_outputs.output_group,
        matrix_outputs.requires_gr,
        matrix_outputs.requires_gss,
        matrix_outputs.requires_gis,
        kd_outputs.gr_status,
        kd_outputs.gss_status,
        kd_outputs.gis_status,
        kd_outputs.reviewed_on,
        kd_outputs.review_ref,
        coalesce(
            (NOT matrix_outputs.requires_gr OR kd_outputs.gr_status = 'agreed')
            AND (NOT matrix_outputs.requires_gss OR kd_outputs.gss_status = 'agreed')
            AND (NOT matrix_outputs.requires_gis OR kd_outputs.gis_status = 'agreed'),
            FALSE
        ) AS approved
    FROM kd_outputs
    LEFT JOIN matrix_outputs ON kd_outputs.output = matrix_outputs.output
{% endmacro %}

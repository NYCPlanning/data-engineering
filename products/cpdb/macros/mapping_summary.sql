{#
    Counts of projects by how they were mapped, grouped by `group_columns` plus a final
    `total` row. The project counts from projects_mapped_as_points to
    projects_no_geometry_found split each row's projects, so they add up to projects_total.
#}
{% macro mapping_summary(group_columns) %}
WITH grouped AS (
    SELECT
        {% for column in group_columns -%}
            {{ column }},
        {% endfor -%}
        GROUPING({{ group_columns | join(', ') }}) AS is_total,
        COUNT(*) FILTER (WHERE also_found_by_another_method) AS projects_also_found_by_another_method,
        COALESCE(SUM(plannedcommit_total) FILTER (WHERE mapped_as IS NOT NULL), 0) AS planned_commitments_mapped,
        COALESCE(SUM(plannedcommit_total) FILTER (WHERE mapped_as IS NULL), 0) AS planned_commitments_not_mapped,
        SUM(plannedcommit_total) AS planned_commitments_total,
        COALESCE(SUM(planned_commitment_records) FILTER (WHERE mapped_as IS NOT NULL), 0) AS planned_commitment_records_mapped,
        COALESCE(SUM(planned_commitment_records) FILTER (WHERE mapped_as IS NULL), 0) AS planned_commitment_records_not_mapped,
        SUM(planned_commitment_records) AS planned_commitment_records_total,
        COUNT(*) FILTER (WHERE mapped_as IS NOT NULL) AS projects_mapped,
        COUNT(*) FILTER (WHERE geom_removed_reason IS NOT NULL) AS projects_geometry_removed,
        COUNT(*) FILTER (WHERE geometry_source IS NOT NULL) AS projects_geometry_found,
        COUNT(*) FILTER (WHERE mapped_as = 'points') AS projects_mapped_as_points,
        COUNT(*) FILTER (WHERE mapped_as = 'polygons') AS projects_mapped_as_polygons,
        COUNT(*) FILTER (
            WHERE geom_removed_reason = 'flagged bad geometry'
        ) AS projects_geometry_removed_flagged_bad,
        COUNT(*) FILTER (
            WHERE geom_removed_reason = 'agency: can never be mapped'
        ) AS projects_geometry_removed_agency_never_mappable,
        COUNT(*) FILTER (
            WHERE geom_removed_reason = 'agency: can be mapped in future'
        ) AS projects_geometry_removed_agency_mappable_in_future,
        COUNT(*) FILTER (WHERE geometry_source IS NULL) AS projects_no_geometry_found,
        COUNT(*) AS projects_total
    FROM {{ ref('int__project_mapping_outcomes') }}
    GROUP BY GROUPING SETS (({{ group_columns | join(', ') }}), ())
)

SELECT
    {% for column in group_columns -%}
        CASE WHEN is_total > 0 THEN 'total' ELSE COALESCE({{ column }}::text, 'none') END AS {{ column }},
    {% endfor -%}
    projects_also_found_by_another_method,
    planned_commitments_mapped,
    planned_commitments_not_mapped,
    planned_commitments_total,
    planned_commitment_records_mapped,
    planned_commitment_records_not_mapped,
    planned_commitment_records_total,
    projects_mapped,
    projects_geometry_removed,
    projects_geometry_found,
    projects_mapped_as_points,
    projects_mapped_as_polygons,
    projects_geometry_removed_flagged_bad,
    projects_geometry_removed_agency_never_mappable,
    projects_geometry_removed_agency_mappable_in_future,
    projects_no_geometry_found,
    projects_total
FROM grouped
-- csv_export writes rows in table order, which follows this for a freshly created table.
-- Qualified names sort by the grouped values, not the text labels above.
ORDER BY
    is_total,
    {% for column in group_columns %}grouped.{{ column }}{{ ', ' if not loop.last }}{% endfor %}
{% endmacro %}

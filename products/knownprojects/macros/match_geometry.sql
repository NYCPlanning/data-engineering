{#
    Resolve, once per project, the geometry a boundary aggregation should match
    on and whether the >=10% overlap rule applies to it.

    The source scripts made this choice inline in each join's ON clause. A CASE
    expression is not an indexable operator, so the planner could not use the
    GiST index on the boundary table and fell back to a nested loop over every
    (project, boundary) pair. Hoisting it here leaves a bare st_intersects() in
    the join, which the index can serve.

    The 'DCP Planner-Added PROJECTs' casing is preserved verbatim from those
    scripts; it matches no rows.
#}

{% macro match_geometry(source_model) %}

WITH classified AS (
    SELECT
        d.*,
        CASE
            WHEN
                (
                    st_area(d.geometry::geography) > 10000
                    OR d.units_gross > 500
                )
                AND d.source IN (
                    'EDC Projected Projects',
                    'DCP Application',
                    'DCP Planner-Added Projects'
                )
                THEN 'polygon'
            WHEN
                d.record_id IN (
                    SELECT record_id
                    FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.zap_project_many_bbls -- noqa: PRS, TMP
                )
                AND d.record_name LIKE '%SD %'
                THEN 'polygon'
            WHEN
                d.record_name LIKE '%Resilient Housing%'
                AND d.source IN (
                    'DCP Application', 'DCP Planner-Added PROJECTs'
                )
                THEN 'polygon'
            WHEN
                (
                    d.record_name LIKE '%NIHOP%'
                    OR d.record_name LIKE '%NCP%'
                )
                AND d.source IN (
                    'DCP Application', 'DCP Planner-Added PROJECTs'
                )
                THEN 'polygon'
            WHEN
                d.source IN (
                    'Future Neighborhood Studies',
                    'Neighborhood Study Projected Development Sites'
                )
                THEN 'polygon'
            WHEN st_area(d.geometry) > 0 THEN 'centroid'
            ELSE 'point'
        END AS match_mode
    FROM {{ ref(source_model) }} AS d
)

SELECT
    *,
    match_mode = 'polygon' AS polygon_mode,
    CASE
        WHEN match_mode = 'centroid' THEN st_centroid(geometry)
        ELSE geometry
    END AS match_geom
FROM classified

{% endmacro %}

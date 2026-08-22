{#
    Allocate KPDB project records to a boundary geography, one row per
    (project, boundary) pair.

    Replaces seven near-identical ~490-line scripts. Parameters:
      boundary_table  relation holding the boundaries
      boundary_cols   list of {source, alias} passed through from that relation
      suffix          names the output columns (proportion_in_<suffix>, ...)
      source_model    project records to allocate; must be the model that
                      match_model was built from
      match_model     precomputed match geometry for source_model

    Projects that overlap several boundaries have their units split by area
    share; the shares are then renormalised so each project's units still sum to
    its total even when a <10% sliver was dropped.
#}

{% macro longform_by_boundary(
    boundary_table,
    boundary_cols,
    suffix,
    source_model='kpdb_deduplicated',
    match_model='kpdb_match_geom'
) %}

WITH boundaries AS (
    SELECT
        geometry,
        {% for col in boundary_cols -%}
        {{ col.source }} AS {{ col.alias }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ source('recipe_sources', boundary_table) }}
),

/*
    The join predicate is a bare st_intersects() so the boundary's GiST index is
    usable; polygon_mode projects then keep only boundaries they cover >= 10% of.
*/
matched AS (
    SELECT
        a.*,
        b.geometry AS {{ suffix }}_geom,
        {% for col in boundary_cols -%}
        b.{{ col.alias }},
        {% endfor -%}
        st_distance(
            a.geometry::geography, b.geometry::geography
        ) AS {{ suffix }}_distance
    FROM {{ ref(match_model) }} AS a
    LEFT JOIN boundaries AS b
        ON
            st_intersects(a.match_geom, b.geometry)
            AND (
                NOT a.polygon_mode
                OR (
                    st_area(st_intersection(a.geometry, b.geometry))
                    / st_area(a.geometry)
                )::decimal >= .1
            )
),

multi_geocoded_projects AS (
    SELECT
        source,
        record_id
    FROM matched
    GROUP BY source, record_id
    HAVING count(*) > 1
),

with_proportion AS (
    SELECT
        a.*,
        CASE
            WHEN
                concat(a.source, a.record_id) IN (
                    SELECT concat(source, record_id)
                    FROM multi_geocoded_projects
                )
                AND st_area(a.geometry) > 0
                THEN
                    (
                        st_area(
                            st_intersection(a.geometry, a.{{ suffix }}_geom)
                        )
                        / st_area(a.geometry)
                    )::decimal
            ELSE 1
        END AS proportion_raw
    FROM matched AS a
),

total_proportion AS (
    SELECT
        source,
        record_id,
        sum(proportion_raw) AS total_proportion
    FROM with_proportion
    GROUP BY source, record_id
),

normalised AS (
    SELECT
        a.*,
        CASE
            WHEN b.total_proportion IS NOT NULL
                THEN (a.proportion_raw / b.total_proportion)::decimal
            ELSE 1
        END AS proportion_final
    FROM with_proportion AS a
    LEFT JOIN total_proportion AS b
        ON a.record_id = b.record_id AND a.source = b.source
),

/*
    Projects matching no boundary have a NULL distance and drop out here, which
    is what leaves them for the fallback in the model's post-hook.
*/
min_distances AS (
    SELECT
        record_id,
        min({{ suffix }}_distance) AS min_distance
    FROM normalised
    GROUP BY record_id
),

closest AS (
    SELECT a.*
    FROM normalised AS a
    INNER JOIN min_distances AS b
        ON
            a.record_id = b.record_id
            AND a.{{ suffix }}_distance = b.min_distance
)

SELECT
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
    a.future_phased_units_total,
    a.future_units_without_phasing,
    a.completed_units,
    a.prop_within_5_years,
    a.prop_5_to_10_years,
    a.prop_after_10_years,
    a.within_5_years,
    a.from_5_to_10_years,
    a.after_10_years,
    a.phasing_rationale,
    a.phasing_known,
    a.classb,
    a.nycha,
    a.senior_housing,
    a.inactive,
    a.geometry,
    {% for col in boundary_cols -%}
    b.{{ col.alias }},
    {% endfor -%}
    b.proportion_final AS proportion_in_{{ suffix }},
    round(a.units_net * b.proportion_final) AS units_net_in_{{ suffix }},
    round(
        a.future_phased_units_total * b.proportion_final
    ) AS future_phased_units_total_in_{{ suffix }},
    round(
        a.future_units_without_phasing * b.proportion_final
    ) AS future_units_without_phasing_in_{{ suffix }},
    round(
        a.completed_units * b.proportion_final
    ) AS completed_units_in_{{ suffix }},
    round(
        b.proportion_final * a.within_5_years::decimal
    ) AS within_5_years_in_{{ suffix }},
    round(
        b.proportion_final * a.from_5_to_10_years::decimal
    ) AS from_5_to_10_years_in_{{ suffix }},
    round(
        b.proportion_final * a.after_10_years::decimal
    ) AS after_10_years_in_{{ suffix }}
FROM {{ ref(source_model) }} AS a
LEFT JOIN closest AS b
    ON a.source = b.source AND a.record_id = b.record_id
ORDER BY
    a.source ASC,
    a.record_id ASC,
    a.record_name ASC,
    a.status ASC

{% endmacro %}

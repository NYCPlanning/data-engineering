-- Community districts, census tracts, and NTAs all nest in boroughs, so a
-- project's units should land in the same borough whichever one it's allocated
-- by. A difference means a boundary file crosses borough lines (the
-- water-included files reach across the East River) or a project matched no
-- boundary, which shows up here as a null borough.
--
-- Tracts take their borough from their NTA, not their own ID. Marble Hill's
-- tract is coded Manhattan (New York County), but its NTA and community
-- district are in the Bronx.
{% set measures = [
    'units_net', 'completed_units', 'within_5_years', 'from_5_to_10_years', 'after_10_years'
] %}

WITH nta_borough AS (
    SELECT
        nta2020,
        borocode::text AS borough
    FROM {{ source('recipe_sources', 'dcp_nta2020') }}
),

nta AS (
    SELECT
        b.borough,
        {% for m in measures -%}
        sum(l.{{ m }}_in_nta) AS {{ m }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('longform_nta_output') }} AS l
    LEFT JOIN nta_borough AS b ON l.nta = b.nta2020
    GROUP BY 1
),

cd AS (
    SELECT
        left(cd::text, 1) AS borough,
        {% for m in measures -%}
        sum({{ m }}_in_cd) AS {{ m }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('longform_cd_output') }}
    GROUP BY 1
),

ct AS (
    SELECT
        b.borough,
        {% for m in measures -%}
        sum(l.{{ m }}_in_ct) AS {{ m }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('longform_ct_output') }} AS l
    LEFT JOIN {{ source('recipe_sources', 'dcp_ct2020') }} AS t ON l.ct = t.boroct2020
    LEFT JOIN nta_borough AS b ON t.nta2020 = b.nta2020
    GROUP BY 1
)

{% for geo in ['cd', 'ct'] %}
SELECT
    '{{ geo }}' AS geography,
    coalesce(nta.borough, {{ geo }}.borough) AS borough,
    {% for m in measures -%}
    {{ geo }}.{{ m }} - nta.{{ m }} AS {{ m }}_difference{{ "," if not loop.last }}
    {% endfor %}
FROM nta
FULL JOIN {{ geo }} ON nta.borough = {{ geo }}.borough
WHERE
    {% for m in measures -%}
    {{ geo }}.{{ m }} IS DISTINCT FROM nta.{{ m }}{{ " OR" if not loop.last }}
    {% endfor %}
{{ "UNION ALL" if not loop.last }}
{% endfor %}

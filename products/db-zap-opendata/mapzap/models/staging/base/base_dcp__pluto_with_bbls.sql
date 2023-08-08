{% set pluto_versions_query %}
select
    primary_pluto_version
from
    {{ ref('seed_pluto_versions') }}
where
    has_bbl
    and has_wkt_geometry
order by 1
{% endset %}

{% set pluto_versions_query_result = run_query(pluto_versions_query) %}

{% if execute %}
    {# Return the first column #}
    {% set pluto_versions = pluto_versions_query_result.columns[0].values() %}
{% else %}
    {% set pluto_versions = [] %}
{% endif %}

{% for pluto_table in pluto_versions %}

    SELECT
        '{{ pluto_table }}' AS pluto_version,
        CAST(bbl AS STRING) AS bbl,
        wkt
    FROM {{ source('map_pluto', pluto_table) }}

    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}

{% set pluto_versions_query %}
select
    primary_pluto_version
from
    {{ ref('seed_pluto_versions') }}
where
    not has_bbl
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

with pluto_union as (
    {% for pluto_table in pluto_versions %}
        SELECT
            '{{ pluto_table }}' AS pluto_version,
            CAST(borocode AS STRING) AS borocode,
            CAST(block AS STRING) AS block,
            CAST(lot AS STRING) AS lot,
            wkt
        FROM {{ source('map_pluto', pluto_table) }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
),

pluto_without_bbls as(
    select
        pluto_version,
        -- The only records where borocode is NULL are
        -- Staten Island records in map_pluto.02b
        IFNULL(borocode, "5") AS borocode,
        block,
        lot,
        wkt
    from pluto_union
)

select * from pluto_without_bbls

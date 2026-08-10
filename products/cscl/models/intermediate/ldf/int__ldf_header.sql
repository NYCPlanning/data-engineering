{{ config(materialized = 'table') }}

/*
The LDF's single header ('H') record.

Two of its fields cannot be derived from source data and are supplied as vars: the two
LION release IDs and the dates they were deployed. GR's tool prompts an operator for the
same four values.

The third, the cumulative record number, is mechanical and is *not* taken as input here.
LDF record numbers run consecutively across editions forever, so this edition starts
where the last one stopped: previous header's number plus its record count. GR types this
in by hand, which is how the published sequence acquired a gap (data_issues.md
CSCL-LDF-03).
*/

{% set required = [
    'ldf_old_release', 'ldf_old_release_date', 'ldf_new_release', 'ldf_new_release_date'
] %}
{# Guarded on `execute` so a missing var fails this model at run time rather than
   breaking parsing of the whole project for anyone building something else. #}
{% if execute %}
    {% for name in required %}
        {% if not var(name, none) %}
            {{ exceptions.raise_compiler_error(
                "var '" ~ name ~ "' must be set to build the LDF header. Pass all of "
                ~ required | join(', ') ~ " via --vars."
            ) }}
        {% endif %}
    {% endfor %}
{% endif %}

WITH previous_edition AS (
    SELECT
        cumulative_record_number::bigint AS cumulative_record_number,
        record_count::int AS record_count
    FROM {{ source('production_outputs', 'previous_ldf_header') }}
),

-- Counts the header itself, matching the spec's "includes header record"
this_edition AS (
    SELECT
        (SELECT count(*) FROM {{ ref('int__ldf_nodes') }})
        + (SELECT count(*) FROM {{ ref('int__ldf_segments') }})
        + 1 AS record_count
)

SELECT
    'H' AS record_type,
    '{{ var("ldf_old_release") }}' AS old_lion_release,
    to_char(date '{{ var("ldf_old_release_date") }}', 'MMDDYYYY') AS old_lion_release_date,
    '{{ var("ldf_new_release") }}' AS new_lion_release,
    to_char(date '{{ var("ldf_new_release_date") }}', 'MMDDYYYY') AS new_lion_release_date,
    this_edition.record_count,
    previous_edition.cumulative_record_number
    + previous_edition.record_count AS cumulative_record_number
FROM previous_edition
CROSS JOIN this_edition

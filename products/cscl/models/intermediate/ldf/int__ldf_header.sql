{{ config(materialized = 'table') }}

/*
The LDF's single header ('H') record.

Two of its fields cannot be derived from source data and are supplied via seeds/config.csv:
the two LION release IDs and the dates they were deployed. GR's tool prompts an operator
for the same four values. ldf_new_release/ldf_new_release_date describe *this* release, and
must be kept in sync with recipe.yml's top-level `version` by hand when it's bumped - see
that key's description in seeds.yml.

The third, the cumulative record number, is mechanical and is *not* taken as input here.
LDF record numbers run consecutively across editions forever, so this edition starts
where the last one stopped: previous header's number plus its record count. GR types this
in by hand, which is how the published sequence acquired a gap (data_issues.md
CSCL-LDF-03).
*/

{% set required_keys = [
    'ldf_previous_version', 'ldf_old_release_date', 'ldf_new_release', 'ldf_new_release_date'
] %}
{# Guarded on `execute` so a missing seed row fails this model at run time rather than
   breaking parsing of the whole project for anyone building something else. #}
{% if execute %}
    {% set present = run_query(
        "SELECT key FROM " ~ ref('config') ~ " WHERE key IN ('" ~ required_keys | join("','") ~ "')"
    ).columns['key'].values() | list %}
    {% set missing = required_keys | reject('in', present) | list %}
    {% if missing %}
        {{ exceptions.raise_compiler_error(
            "seeds/config.csv is missing " ~ missing | join(', ') ~
            " - all of " ~ required_keys | join(', ') ~ " must be present to build the LDF header."
        ) }}
    {% endif %}
{% endif %}

WITH previous_edition AS (
    SELECT
        cumulative_record_number::bigint AS cumulative_record_number,
        record_count::int AS record_count
    FROM {{ source('recipe_sources', 'previous_ldf_header') }}
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
    upper({{ config_value('ldf_previous_version') }}) AS old_lion_release,
    to_char({{ config_value('ldf_old_release_date') }}::date, 'MMDDYYYY') AS old_lion_release_date,
    upper({{ config_value('ldf_new_release') }}) AS new_lion_release,
    to_char({{ config_value('ldf_new_release_date') }}::date, 'MMDDYYYY') AS new_lion_release_date,
    this_edition.record_count,
    previous_edition.cumulative_record_number
    + previous_edition.record_count AS cumulative_record_number
FROM previous_edition
CROSS JOIN this_edition

-- Prod's ETL log entries (production_outputs.etl_log, built by build_prod_snapshot.py),
-- each matched to the prod message template it was written from (seed
-- prod_log_templates). Every template's pattern has three capture groups: the feature
-- class or table, the record's id (an OBJECTID, or a source id like STREETNAMEID), and any
-- other key (a lookup query, a place name). A group is empty where the message has none.
-- Snapshots built before etl_log existed have no such table: then this is empty.
{%- set etl_log = adapter.get_relation(
    database=target.database,
    schema=var("production_outputs_schema"),
    identifier="etl_log"
) if execute else none %}

WITH etl_log AS (
    {% if etl_log %}
        SELECT
            log_file,
            line_number,
            logged_at,
            log_level,
            message
        FROM {{ etl_log }}
    {% else %}
        SELECT
            NULL::VARCHAR AS log_file,
            NULL::BIGINT AS line_number,
            NULL::VARCHAR AS logged_at,
            NULL::VARCHAR AS log_level,
            NULL::VARCHAR AS message
        WHERE FALSE
    {% endif %}
),
templates AS (
    SELECT * FROM {{ ref("prod_log_templates") }}
)
SELECT
    etl_log.log_file,
    etl_log.line_number,
    etl_log.logged_at,
    etl_log.log_level,
    templates.template_id,
    templates.prod_output,
    NULLIF(templates.our_error_category, '') AS our_error_category,
    NULLIF(REGEXP_EXTRACT(etl_log.message, templates.pattern, 1), '') AS feature_class,
    NULLIF(REGEXP_EXTRACT(etl_log.message, templates.pattern, 2), '') AS record_id,
    NULLIF(REGEXP_EXTRACT(etl_log.message, templates.pattern, 3), '') AS record_key,
    etl_log.message
FROM etl_log
LEFT JOIN templates ON REGEXP_FULL_MATCH(etl_log.message, templates.pattern)

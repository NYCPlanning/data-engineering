-- CSCL-LOG-OURS-26, every build: how many RPL.txt generics have their roadbeds in a
-- different order by prod's rule (each roadbed on the side of the generic its midpoint
-- lies on, outermost first) than by the spec's (by ROADBED_POSITION_CODE), i.e. what the
-- var fix_rpl_roadbed_order changes (CSCL-RPL-02), and whether it is on. One row, also
-- when the count is 0.
WITH changed AS (
    SELECT
        generic_segmentid,
        count(*) AS roadbed_rows
    FROM {{ ref('int__rpl') }}
    WHERE prod_group_order_id <> spec_group_order_id
    GROUP BY generic_segmentid
)
SELECT
    'warning' AS log_level,
    'rpl roadbed order' AS error_category,
    'CSCL-LOG-OURS-26' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_rpl_roadbed_order') -%}
    {{ log_message(
        "{} RPL generics ({} roadbed rows) have their roadbeds in a different order by "
        "prod's rule (each roadbed on the side of the generic its midpoint lies on, "
        "outermost first) than by the spec's (R, inner roadbeds on its side, L, the other "
        "inner roadbeds, by ROADBED_POSITION_CODE). The spec's roadbed order is ON in this "
        "build (fix_rpl_roadbed_order = true): RPL.txt has the spec's order (CSCL-RPL-02)",
        ["count(*)", "coalesce(sum(roadbed_rows), 0)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} RPL generics ({} roadbed rows) have their roadbeds in a different order by "
        "prod's rule (each roadbed on the side of the generic its midpoint lies on, "
        "outermost first) than by the spec's (R, inner roadbeds on its side, L, the other "
        "inner roadbeds, by ROADBED_POSITION_CODE). The spec's roadbed order is OFF in this "
        "build (fix_rpl_roadbed_order = false): RPL.txt has prod's order (CSCL-RPL-02)",
        ["count(*)", "coalesce(sum(roadbed_rows), 0)"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed

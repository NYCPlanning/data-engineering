-- Each prod_log_templates example (a message prod's code writes) matches its own pattern
-- and no other, so a prod log line is never parsed by two templates.
WITH templates AS (
    SELECT * FROM {{ ref("prod_log_templates") }}
),
matches AS (
    SELECT
        example.template_id,
        LIST(pattern.template_id ORDER BY pattern.template_id) AS matched_by
    FROM templates AS example
    LEFT JOIN templates AS pattern ON REGEXP_FULL_MATCH(example.example, pattern.pattern)
    GROUP BY example.template_id
)
SELECT *
FROM matches
WHERE matched_by <> [template_id]

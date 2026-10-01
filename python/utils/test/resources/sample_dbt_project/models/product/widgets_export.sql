{{ config(materialized='table') }}

-- Its dependency on widgets_export_by_field is intentionally hidden behind a macro
-- argument rather than a literal ref() - see macros/pull_named_model.sql.
SELECT widget_id || widget_name || status AS dat_row
FROM {{ pull_named_model('widgets_export_by_field') }}

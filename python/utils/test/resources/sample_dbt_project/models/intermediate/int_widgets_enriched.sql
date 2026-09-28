-- Deliberately has no schema yml entry - exercises the "no documentation yet" path.
SELECT
    widget_id,
    widget_name,
    'enriched' AS status
FROM {{ ref('stg_widgets') }}

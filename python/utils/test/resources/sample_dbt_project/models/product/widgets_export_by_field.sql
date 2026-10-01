{{ config(tags=['export_doc'], meta={'group': 'widgets'}) }}

SELECT
    widget_id,
    widget_name,
    status
FROM {{ ref('int_widgets_enriched') }}

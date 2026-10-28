-- The layout's fields, unformatted, for the RPL.txt export (recipe.yml: layout_source).
-- group_order_id isn't a layout field (the writer ignores it); it's the within-generic
-- roadbed order the file is written in (recipe.yml: order_by).
SELECT
    {{ select_fields_from_seed('text_formatting__rpl') }},
    group_order_id
FROM {{ ref("int__rpl") }}
ORDER BY generic_segmentid ASC, group_order_id ASC

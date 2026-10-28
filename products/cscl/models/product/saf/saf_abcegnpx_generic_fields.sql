-- Unformatted rows of saf_abcegnpx_generic_by_field, for packaging (dcpy.utils.fixed_width).
-- saf_block, saf_source_objectid, rpl_flag: the file's record order (recipe.yml
-- order_by): source tables in prod's cursor order, each in OBJECTID order, a record's
-- RPL generic copy right after it. The layout ignores them.
SELECT
    {{ select_fields_from_seed('text_formatting__saf_a') }},
    1 AS saf_block,
    saf_source_objectid,
    rpl_flag
FROM {{ ref("int__saf_abcep" ) }}
WHERE generic
UNION ALL
SELECT
    {{ select_fields_from_seed('text_formatting__saf_a') }},
    2 AS saf_block,
    saf_source_objectid,
    rpl_flag
FROM {{ ref("int__saf_gnx" ) }}
WHERE generic

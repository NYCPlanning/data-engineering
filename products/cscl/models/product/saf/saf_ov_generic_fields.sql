-- Unformatted rows of saf_ov_generic_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__saf_v') }}
FROM {{ ref("int__saf_o" ) }}
WHERE generic
UNION ALL
SELECT
    {{ select_fields_from_seed('text_formatting__saf_v') }}
FROM {{ ref("int__saf_v" ) }}
WHERE generic

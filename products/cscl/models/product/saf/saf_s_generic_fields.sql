-- Unformatted rows of saf_s_generic_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__saf_s') }}
FROM {{ ref("int__saf_s" ) }}
WHERE generic

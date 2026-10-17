-- Unformatted rows of saf_d_generic_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__saf_d') }}
FROM {{ ref("int__saf_d" ) }}
WHERE generic

-- Unformatted rows of saf_i_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__saf_i') }}
FROM {{ ref("int__saf_i" ) }}

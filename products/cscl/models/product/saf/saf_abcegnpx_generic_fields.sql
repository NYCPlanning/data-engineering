-- Unformatted rows of saf_abcegnpx_generic_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__saf_a') }}
FROM {{ ref("int__saf_abcep" ) }}
WHERE generic
UNION ALL
SELECT
    {{ select_fields_from_seed('text_formatting__saf_a') }}
FROM {{ ref("int__saf_gnx" ) }}
WHERE generic

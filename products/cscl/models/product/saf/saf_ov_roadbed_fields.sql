-- Unformatted rows of saf_ov_roadbed_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__saf_v') }}
FROM {{ ref("int__saf_o" ) }}
WHERE roadbed
UNION ALL
SELECT
    {{ select_fields_from_seed('text_formatting__saf_v') }}
FROM {{ ref("int__saf_v" ) }}
WHERE roadbed

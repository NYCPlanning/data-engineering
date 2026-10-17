-- LION .dat rows unformatted, for packaging: the borough files are formatted from this
-- (exported to parquet) by dcpy.utils.fixed_width with text_formatting__lion_dat. Same
-- rows as lion_dat_by_field.
SELECT
    {{ select_fields_from_seed('text_formatting__lion_dat') }}
FROM {{ ref("int__lion") }}
WHERE include_in_geosupport_lion

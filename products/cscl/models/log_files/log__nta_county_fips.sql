-- CSCL-LOG-OURS-16, every build: how many nynta2010 NTAs get a different CountyFIPS from
-- prod's lookup (their NeighborhoodPUMACodes FIPS code, blank where it is blank) than from
-- their borough (CSCL-DISTRICTS-09, var fill_nta_county_fips_from_borough), which NTAs, and
-- which one the build writes. One row, also when the count is 0.
WITH changed AS (
    SELECT nta2010
    FROM {{ ref('int__nta2010_county_fips') }}
    WHERE county_fips_prod IS DISTINCT FROM county_fips_borough
)
SELECT
    'warning' AS log_level,
    'nta county fips' AS error_category,
    'CSCL-LOG-OURS-16' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'nynta2010' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fill_nta_county_fips_from_borough') -%}
    {{ log_message(
        "{} nynta2010 NTAs ({}) have a blank or no NeighborhoodPUMACodes FIPS code, or one that isn't their borough's. "
        "Filling CountyFIPS from the borough is ON in this build (fill_nta_county_fips_from_borough = true): "
        "they get their borough's FIPS code, where prod leaves CountyFIPS blank or writes the lookup's (CSCL-DISTRICTS-09)",
        ["count(*)", "coalesce(string_agg(nta2010, ', ' ORDER BY nta2010), 'none')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} nynta2010 NTAs ({}) have a blank or no NeighborhoodPUMACodes FIPS code, or one that isn't their borough's. "
        "Filling CountyFIPS from the borough is OFF in this build (fill_nta_county_fips_from_borough = false): "
        "they get the lookup's, blank where it is blank, as prod does (CSCL-DISTRICTS-09)",
        ["count(*)", "coalesce(string_agg(nta2010, ', ' ORDER BY nta2010), 'none')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed

-- Every report row, including months that appear in more than one report. Values are text in the source.
-- Removal is a fraction in most reports, but some write it as 97%, 97, or 0.0097, so it's rescaled to a
-- fraction and the reported value is kept beside it.
{%- set removal_columns = ['tss_removal', 'cbod_removal'] %}
SELECT
    source_file,
    sheet_name,
    strptime(regexp_replace(trim(sheet_name), '\s+(old|new)$', ''), '%B %Y')::date AS report_month,
    regexp_extract(trim(sheet_name), '\s+(old|new)$', 1) AS sheet_version,
    {{ wastewater_facility_id('plant') }} AS wastewater_facility_id,
    flow_mgd::double AS flow_mgd,
    flow_12_month_average_mgd::double AS flow_12_month_average_mgd,
    flow_permit_mgd::double AS flow_permit_mgd,
    tss_influent_mg_l::double AS tss_influent_mg_l,
    tss_effluent_qualifier,
    tss_effluent_mg_l::double AS tss_effluent_mg_l,
    tss_effluent_permit_mg_l::double AS tss_effluent_permit_mg_l,
    tss_removal_qualifier,
    cbod_influent_mg_l::double AS cbod_influent_mg_l,
    cbod_effluent_qualifier,
    cbod_effluent_mg_l::double AS cbod_effluent_mg_l,
    cbod_effluent_permit_mg_l::double AS cbod_effluent_permit_mg_l,
    cbod_removal_qualifier,
{%- for column in removal_columns %}
    CASE
        WHEN {{ column }} LIKE '%\%' ESCAPE '\' THEN replace({{ column }}, '%', '')::double / 100
        WHEN {{ column }}::double > 1 THEN {{ column }}::double / 100
        WHEN {{ column }}::double < 0.05 THEN {{ column }}::double * 100
        ELSE {{ column }}::double
    END AS {{ column }},
    {{ column }} AS {{ column }}_as_reported,
    {{ column }}_permit::double AS {{ column }}_permit,
{%- endfor %}
FROM {{ source('sources', 'dep_wastewater_performance') }}

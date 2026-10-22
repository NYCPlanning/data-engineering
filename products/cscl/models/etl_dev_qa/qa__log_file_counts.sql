-- Record counts per output file three ways: prod's "Total <name> output records: N" log
-- lines, prod's file itself (production_outputs), and ours (log__export_file_summary).
-- Only files prod's received logs report on. Prod names a file by its extract, not its
-- file name; names not in the mapping below show with a blank output_filename.
{%- set prod_files = [
    ('SAFI', 'Intersection.txt', 'saf_i'),
    ('SAFS_Generic', 'GenericS.txt', 'saf_s_generic'),
    ('SAFS_Roadbed', 'RoadbedS.txt', 'saf_s_roadbed'),
    ('SAFD_Generic', 'GenericD.txt', 'saf_d_generic'),
    ('SAFD_Roadbed', 'RoadbedD.txt', 'saf_d_roadbed'),
    ('SAFV_Generic', 'GenericOV.txt', 'saf_ov_generic'),
    ('SAFV_Roadbed', 'RoadbedOV.txt', 'saf_ov_roadbed'),
    ('SAFA_Generic', 'GenericABCEGNPX.txt', 'saf_abcegnpx_generic'),
    ('SAFA_Roadbed', 'RoadbedABCEGNPX.txt', 'saf_abcegnpx_roadbed'),
    ('SND', 'SND.txt', 'snd'),
    ('ENDERS', 'Enders.txt', 'enders'),
    ('EXCEPTION', 'Exception.txt', 'exception'),
    ('RPL', 'RPL.txt', 'rpl'),
] %}

WITH summary_lines AS (
    SELECT
        log_file,
        line_number,
        -- SAF S/D/V/A lines carry two files: "Total X output records: N and Y output records: M"
        REGEXP_EXTRACT_ALL(message, '(?:Total|and) (.+?) output records: (\d+)', 1) AS names,
        REGEXP_EXTRACT_ALL(message, '(?:Total|and) (.+?) output records: (\d+)', 2) AS counts
    FROM {{ ref("qa_int__prod_etl_log") }}
    WHERE template_id = 'output_file_summary'
),
prod_logged AS (
    SELECT
        log_file,
        line_number,
        UNNEST(names) AS prod_name,
        UNNEST(counts)::INT AS prod_logged_records
    FROM summary_lines
),
file_names AS (
    {% for name, filename, table in prod_files %}
        SELECT
            '{{ name }}' AS prod_name,
            '{{ filename }}' AS output_filename,
            (SELECT COUNT(*) FROM {{ source("production_outputs", table) }}) AS prod_file_records
        {{ "UNION ALL" if not loop.last }}
    {% endfor %}
),
ours AS (
    SELECT
        output_filename,
        record_count
    FROM {{ ref("log__export_file_summary") }}
)
SELECT
    prod_logged.prod_name,
    file_names.output_filename,
    prod_logged.log_file,
    prod_logged.prod_logged_records,
    file_names.prod_file_records,
    ours.record_count AS our_records,
    ours.record_count - prod_logged.prod_logged_records AS our_minus_prod_logged,
    ours.record_count - file_names.prod_file_records AS our_minus_prod_file,
    COALESCE(ours.record_count = prod_logged.prod_logged_records, FALSE) AS counts_match
FROM prod_logged
LEFT JOIN file_names ON prod_logged.prod_name = file_names.prod_name
LEFT JOIN ours ON file_names.output_filename = ours.output_filename

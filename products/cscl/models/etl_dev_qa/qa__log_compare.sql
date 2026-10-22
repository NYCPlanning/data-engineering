-- Our build log (log.csv) vs prod's ETL logs, one row per kind of logged event. A
-- category is our error_category, or, for a prod message we don't log, its template_id
-- (seed prod_log_templates). Prod's counts cover only the ETL sessions in its etl_log
-- (prod_log_files); ours are the whole build.
--
-- Records can't be matched one to one except output file summaries: prod names a record
-- by its OBJECTID in the source FGDB, which the build doesn't keep (our ogc_fid numbers
-- rows 1..N, not by OBJECTID). Those categories compare counts only.
WITH prod AS (
    SELECT * FROM {{ ref("qa_int__prod_etl_log") }}
),
ours AS (
    SELECT * FROM {{ ref("log") }}
),
templates AS (
    SELECT
        COALESCE(NULLIF(our_error_category, ''), template_id) AS category,
        STRING_AGG(template_id, ', ' ORDER BY template_id) AS prod_templates,
        STRING_AGG(DISTINCT prod_output, ', ' ORDER BY prod_output) AS prod_output,
        BOOL_OR(our_error_category <> '') AS we_log_it,
        STRING_AGG(NULLIF(note, ''), ' / ' ORDER BY template_id) AS note
    FROM {{ ref("prod_log_templates") }}
    GROUP BY category
),
prod_counts AS (
    SELECT
        COALESCE(our_error_category, template_id, 'unmatched prod message') AS category,
        COUNT(*) AS prod_lines,
        COUNT(DISTINCT CONCAT_WS('|', feature_class, record_id, record_key)) AS prod_records,
        STRING_AGG(DISTINCT log_level, ', ' ORDER BY log_level) AS prod_log_level,
        STRING_AGG(DISTINCT log_file, ', ' ORDER BY log_file) AS prod_log_files
    FROM prod
    GROUP BY category
),
our_counts AS (
    SELECT
        error_category AS category,
        COUNT(*) AS our_rows,
        COUNT(DISTINCT COALESCE(globalid, output_filename)) AS our_records,
        STRING_AGG(DISTINCT log_level, ', ' ORDER BY log_level) AS our_log_level
    FROM ours
    GROUP BY category
),
file_records AS (
    SELECT
        'output file summary' AS category,
        COUNT(*) FILTER (WHERE counts_match) AS records_matched,
        COUNT(*) FILTER (WHERE NOT counts_match) AS records_prod_only,
        COUNT(*) FILTER (WHERE NOT counts_match AND our_records IS NOT NULL) AS records_ours_only
    FROM {{ ref("qa__log_file_counts") }}
),
categories AS (
    SELECT category FROM templates
    UNION
    SELECT category FROM prod_counts
    UNION
    SELECT category FROM our_counts
)
SELECT
    categories.category,
    CASE
        WHEN templates.we_log_it THEN 'both'
        WHEN templates.category IS NOT NULL THEN 'prod only'
        WHEN categories.category = 'unmatched prod message' THEN 'prod only'
        ELSE 'ours only'
    END AS logged_by,
    templates.prod_templates,
    templates.prod_output,
    COALESCE(prod_counts.prod_lines, 0) AS prod_lines,
    COALESCE(prod_counts.prod_records, 0) AS prod_records,
    COALESCE(our_counts.our_rows, 0) AS our_rows,
    COALESCE(our_counts.our_records, 0) AS our_records,
    prod_counts.prod_log_level,
    our_counts.our_log_level,
    CASE
        WHEN file_records.category IS NOT NULL
            THEN 'records: output file and record count (qa__log_file_counts)'
        ELSE 'counts only'
    END AS compared_on,
    file_records.records_matched,
    file_records.records_prod_only,
    file_records.records_ours_only,
    prod_counts.prod_log_files,
    templates.note
FROM categories
LEFT JOIN templates ON categories.category = templates.category
LEFT JOIN prod_counts ON categories.category = prod_counts.category
LEFT JOIN our_counts ON categories.category = our_counts.category
LEFT JOIN file_records ON categories.category = file_records.category
ORDER BY logged_by, category

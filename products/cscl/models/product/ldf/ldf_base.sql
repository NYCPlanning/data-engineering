/*
The LDF record file. Node and segment records use different layouts, so each block is
formatted against its own seed and the two are concatenated - `select_rows_as_text` only
handles one layout at a time.

Ordering is by the cumulative record number already embedded at positions 91-100, so the
file's order and its numbering cannot disagree.
*/

WITH nodes AS (
    {{ select_rows_as_text(model='ldf_n_by_field') }}
),

segments AS (
    {{ select_rows_as_text(model='ldf_segment_by_field') }}
),

combined AS (
    SELECT dat_column FROM nodes
    UNION ALL
    SELECT dat_column FROM segments
)

SELECT dat_column
FROM combined
ORDER BY substring(dat_column, 91, 10)

-- create versions of relvant import data to modify
DROP TABLE IF EXISTS _cbbr_submissions;

CREATE TABLE _cbbr_submissions AS
SELECT
    unique_id,
    trkno AS tracking_code,
    borough,
    RIGHT(cb_label, 2) AS cd,
    cb_label,
    type AS type_br,
    priority,
    need,
    request,
    REPLACE(reason, E'\n', ' ') AS explanation,
    location,
    loc_type AS type,
    supported_by_1 AS supporters_1,
    supported_by_2 AS supporters_2,
    project_id_1,
    project_id_2,
    project_id_3,
    budget_line_1,
    budget_line_2,
    budget_line_3,
    agency_acronym,
    agency,
    agy_response_category AS agency_category_response,
    agy_response AS agency_response,
    REPLACE(additional_comment, E'\n', ' ') AS additional_comment,
    NULL AS parent_tracking_code
FROM cbbr_submissions;

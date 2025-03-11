-- create versions of relvant import data to modify
DROP TABLE IF EXISTS _cbbr_submissions;

CREATE TABLE _cbbr_submissions AS
SELECT
    dcpuniqid AS unique_id,
    trkno AS tracking_code,
    borough,
    RIGHT(cb_label, 2) AS cd,
    cb_label,
    type AS type_br,
    priority,
    need, -- missing
    request,
    REPLACE(reason, E'\n', ' ') AS explanation,
    location,
    loc_type AS type,
    lowsgrp1 AS supporters_1,
    lowsgrp2 AS supporters_2,
    capis1 AS project_id_1, -- all null
    capis2 AS project_id_2,
    capis3 AS project_id_3,
    budline1 AS budget_line_1, -- all null
    budline2 AS budget_line_2,
    budline3 AS budget_line_3,
    agency_acronym,
    agency,
    agyrspcat AS agency_category_response, -- all null
    agency_response,
    REPLACE(explanation, E'\n', ' ') AS additional_comment,
    NULL AS parent_tracking_code
FROM omb_cbbr_agency_responses

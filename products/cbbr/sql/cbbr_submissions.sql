-- create versions of relvant import data to modify
DROP TABLE IF EXISTS _cbbr_submissions;

CREATE TABLE _cbbr_submissions AS
SELECT
    omb.dcpuniqid AS unique_id,
    omb.trkno AS tracking_code,
    omb.borough,
    RIGHT(omb.cb_label, 2) AS cd,
    omb.cb_label,
    dcp.type,
    omb.type AS type_br,
    omb.priority,
    dcp.policy_area,
    dcp.need_group,
    dcp.need,
    dcp.budget_request_title_please_give_your_budget_request_a_short_an AS title,
    dcp.request,
    -- TODO - should probably just use dcp.explanation but clean special charactesr
    REPLACE(omb.reason, E'\n', ' ') AS explanation,
    dcp."is_this_request_specific_to_a_particular_site,_location,_or_fac" AS location_specific,
    dcp."address:request_location_(complete_all_that_apply)_for_addresse" AS address,
    dcp."site_or_facility_name:request_location_(complete_all_that_apply" AS site_or_facility_name,
    dcp."on_street_(enter_street_name):complete_the_following_if_your_re" AS on_street,
    dcp."cross_street_1_(for_street_segment):request_location_(complete_" AS cross_street_1,
    dcp."cross_street_2_(for_street_segment):request_location_(complete_" AS cross_street_2,
    dcp."intersection___street_1:complete_the_following_if_your_request_" AS intersection_street_1,
    dcp."intersection___street_2:complete_the_following_if_your_request_" AS intersection_street_2,
    omb.lowsgrp1 AS supporters_1,
    omb.lowsgrp2 AS supporters_2,
    omb.capis1 AS project_id_1, -- all null
    omb.capis2 AS project_id_2,
    omb.capis3 AS project_id_3,
    omb.budline1 AS budget_line_1, -- all null
    omb.budline2 AS budget_line_2,
    omb.budline3 AS budget_line_3,
    omb.agency_acronym,
    omb.agency,
    REPLACE(omb.explanation, E'\n', ' ') AS agency_response,
    omb.agency_response AS agency_category_response
FROM omb_cbbr_agency_responses AS omb
INNER JOIN dcp_cbbr_requests AS dcp ON omb.dcpuniqid = dcp.current_year_id

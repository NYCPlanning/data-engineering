-- Find ACS variables that are newly introduced.
-- This probably does NOT indicate a problem, but should be used to confirm
-- that new expected variables are present in the draft

SELECT pff_variable, category, "domain"
FROM  {{ source("build_sources", "acs_metadata_current_latest_draft") }}
where pff_variable not in
      (select pff_variable from {{ source("build_sources", "acs_metadata_current_latest_published") }})

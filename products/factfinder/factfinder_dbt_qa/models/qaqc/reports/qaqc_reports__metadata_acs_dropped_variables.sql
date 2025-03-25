-- Find ACS variables that were present in the previous published version,
-- but don't exist in this draft. This probably indicates a problem.

SELECT pff_variable, category, "domain"
FROM  {{ source("build_sources", "acs_metadata_current_latest_published") }}
where pff_variable not in
      (select pff_variable from {{ source("build_sources", "acs_metadata_current_latest_draft") }})

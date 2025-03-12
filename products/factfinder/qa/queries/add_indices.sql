CREATE INDEX acs_current_latest_draft_idx ON ar_build_auto.acs_current_latest_draft (labs_geoid, pff_variable);
CREATE INDEX acs_previous_latest_draft_idx ON ar_build_auto.acs_previous_latest_draft (labs_geoid, pff_variable);


-- c = Coefficient of Variation
-- e = estimate
-- m = margin error

2010_census, Boro2020	1 HHldr:	763846,

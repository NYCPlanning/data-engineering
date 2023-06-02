# Update source data

## Partner Agency Data

> It's important to note that when pulling the most recent data from `db-knownprojects-data` repo that the latest commit of that repo's copy in `db-knownprojects` should match that of the data repo. Without double-checking, you could be pulling stale data. List of source data that should be updated (via `db-knownprojects-data`) but double checked in this repo (after updating the submodule):

- [ ] `Empire State Development` - `esd_projects`
- [ ] `EDC projects for DCP housing projections` - `edc_projects`
- [ ] `EDC Shapefile` - `edc_dcp_inputs`
- [ ] `Neighborhood Study Commitments` - `dcp_n_study`
- [ ] `Future Neighborhood Rezonings` - `dcp_n_study_future` 
- [ ] `Past Neighborhood Rezonings` - `dcp_n_study_projected`
- [ ] `HPD Requests for Proposals (RFPs)` - `hpd_rfp` 
- [ ] `HPD Projected Closing` - `hpd_pc`
- [ ] `DCP Planner Added Projects` - `dcp_planneradded`

**Make sure the following are up-to-date in edm-recipes:**

- [ ]  `dcp_housing` -> check which version is latest and need to be updated before KPDB can be run
- [ ]  `dcp_projects`
- [ ]  `dcp_projectactions`
- [ ]  `dcp_projectbbls`
- [ ]  `dcp_dcpprojecteams`
- [ ]  `dcp_mappluto_wi`
- [ ]  `dcp_boroboundaries_wi`
- [ ]  `dcp_zoningmapamendments`
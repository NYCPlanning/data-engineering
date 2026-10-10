# TODO: CSV Refactor for FACTYPE/FACSUBGRP Assignment

## Goal

Replace hardcoded CASE statements for `factype` and `facsubgrp` assignment in pipeline SQL files with two lookup CSVs. This enforces the one-to-many relationship between `FACTYPE` and `FACSUBGRP` structurally rather than through a dbt test.

## Prerequisites

- The `many_to_one_facdb_export__FACTYPE____FACSUBGRP_` dbt test is **passing** (completed).
- All pipeline SQL files have been fixed so each `factype` maps to exactly one `facsubgrp`.

## Two Lookup CSVs to Create

### 1. `facdb/data/lookup_factype_source.csv`
Maps source values to `factype`:
- Columns: `source_name`, `source_column`, `source_value`, `factype`
- Replaces the per-pipeline CASE statements that assign `factype`

### 2. `facdb/data/lookup_factype.csv`
Maps `factype` to `facsubgrp`:
- Columns: `factype`, `facsubgrp`
- Enforces the one-to-many constraint structurally (one row per factype)
- Do **not** merge this into `lookup_classification.csv` — keep it separate

## Rules

- Add `ELSE NULL` to any remaining CASE statements not covered by the CSVs, so unmapped values fail the `not_null` test rather than silently passing through.
- Do **not** add `factype` to `lookup_classification.csv`.
- All 55 pipelines listed below are in scope — not just the four fixed in the prior phase.

## Pipelines in Scope

All 55 pipeline files that assign `factype` and/or `facsubgrp`:

```
bpl_libraries.sql
dca_operatingbusinesses.sql
dcla_culturalinstitutions.sql
dcp_colp.sql
dcp_pops.sql
dcp_sfpsd.sql
dep_wwtc.sql
dfta_contracts.sql
doe_busroutesgarages.sql
doe_lcgms.sql
doe_universalprek.sql
dohmh_daycare.sql
dot_bridgehouses.sql
dot_ferryterminals.sql
dot_mannedfacilities.sql
dot_pedplazas.sql
dot_publicparking.sql
dpr_parksproperties.sql
dsny_donatenycdirectory.sql
dsny_electronicsdrop.sql
dsny_fooddrop.sql
dsny_garages.sql
dsny_leafdrop.sql
dsny_specialwastedrop.sql
dycd_service_sites.sql
fbop_corrections.sql
fdny_firehouses.sql
foodbankny_foodbanks.sql
hhc_hospitals.sql
hra_jobcenters.sql
hra_medicaid.sql
hra_snapcenters.sql
moeo_socialservicesitelocations.sql
nycdoc_corrections.sql
nycha_communitycenters.sql
nycha_policeservice.sql
nycourts_courts.sql
nypl_libraries.sql
nysdec_lands.sql
nysdec_solidwaste.sql
nysdoccs_corrections.sql
nysdoh_healthfacilities.sql
nysdoh_nursinghomes.sql
nysed_activeinstitutions.sql
nysoasas_programs.sql
nysomh_mentalhealth.sql
nysopwdd_providers.sql
nysparks_historicplaces.sql
nysparks_parks.sql
qpl_libraries.sql
sbs_workforce1.sql
uscourts_courts.sql
usdot_airports.sql
usdot_ports.sql
usnps_parks.sql
```

## Reference

See `facdb/data/lookup_classification.csv` and `facdb/sql/_create_facdb_classification.sql` for the existing pattern to follow when wiring up new CSVs.

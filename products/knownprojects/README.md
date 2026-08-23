# Known Projects Database (KPDB)

KPDB shows where new housing is expected in New York City, project by project.
It pulls projects from nine sources, figures out which records describe the same
project so units don't get counted twice, applies assumptions about when the
units will get built, and adds everything up by geography.

It's used for growth planning citywide and by neighborhood, and it's what the
School Construction Authority's Housing Pipeline is built from.

> **Disclaimer**
> This is not a housing projection produced by DCP, and DCP can't say that each
> of these developments will actually lead to housing being built. KPDB doesn't
> cover future as-of-right growth, or growth from projects that haven't shown up
> in the sources below yet.

KPDB is experimental, gets built about once a year, and isn't published
publicly. Builds go to `edm-publishing` under `db-kpdb/build/<build name>`.

## Data sources

Recipe inputs are versioned in `edm-recipes` and listed in `recipe.yml`. Agency
files are put together by hand, read from `edm-private`, and named with their
date in `python/__init__.py`.

| Source | From | How it arrives |
|---|---|---|
| DOB permits and jobs | [Housing Database](../developments) (`dcp_housing`) | recipe, pinned |
| DCP Applications | ZAP (`dcp_projects` and friends) | recipe, pinned by `ZAP_VERSION` |
| HPD RFPs | HPD | agency file |
| EDC projected projects | EDC | agency file |
| Empire State Development | ESD | agency file |
| Neighborhood study rezoning commitments | DCP | agency file |
| Neighborhood study projected sites | DCP | agency file |
| Future neighborhood studies | DCP | agency file |
| DCP planner-added projects | DCP planners | agency file |

The last two are **speculative and not site-specific**. Future neighborhood
studies aren't deduplicated and get a certainty discount, since those rezonings
haven't been adopted. Depending on what you're doing, it can make sense to leave
both out.

Two things to watch out for with dates:

- DOB and DCP Application both get updated constantly and are pinned
  separately, so they can end up covering different periods. Use the latest of
  each. `assert_project_and_dob_data_align` warns if they drift apart.
- Version labels are release tags, not cutoffs. `dcp_housing` `25Q4` has records
  going into January 2026.

## Concepts

**Record.** One row from one source, with a `record_id`. The same building often
shows up as several records: a DOB job, a DCP application, and a planner's note
can all describe it.

**Cluster.** A first pass at grouping non-DOB records that sit next to each
other, using `ST_CLUSTERDBSCAN(geom, 0, 1)`. The distance is zero, so records
only cluster if their geometries actually touch. Clustering chains: if A touches
B and B touches C, all three end up in one cluster even though A and C don't
touch. That's usually why a cluster looks bigger than it should.

A second pass then breaks clusters back up. It takes every place where two
records in the cluster overlap, unions those into one shared area, and keeps
only the records that overlap it, contain it, or sit inside it. A record that
only got into the cluster by touching a neighbor, without touching the shared
area, gets dropped. So a cluster is a starting point, not the answer.

DOB records don't get clustered. They're matched to non-DOB records separately,
by location and with a date check so a DOB job only matches a project from
around the same time. Neighborhood Study Rezoning Commitments and Future
Neighborhood Studies are left out of clustering too, because their units aren't
deduplicated against other sources.

**Project.** The grouping that sticks: a set of records treated as one
development, stored as an array in `project_record_ids` with a `project_id`. A
record that didn't match anything becomes a project by itself. Corrections work
at this level, either moving a record from one project to another or merging two
projects.

**Deduplication.** Within a project, units only get counted once. Sources are
ranked, and each record's units are reduced by whatever the higher-ranked
records in that project already claimed, with zero as the floor:

1. DOB
2. HPD RFPs
3. EDC Projected Projects
4. DCP Application
5. Empire State Development Projected Projects
6. Neighborhood Study Rezoning Commitments
7. Neighborhood Study Projected Development Sites
8. DCP Planner-Added Projects

**Gross and net units.** `units_gross` is what the source said. `units_net` is
what KPDB counts after deduplication, so adding up `units_net` across a project
gives you its units without double counting.

## How the build works

1. **Dataloading.** Recipe inputs through `dcpy`, agency files through
   `01_dataloading.sh`.
2. **Build** (`02_build.sh`). Map each source into a common schema, combine
   them, match records that describe the same project, deduplicate units, and
   make `kpdb`.
3. **Aggregate** (`03_aggregate.sh`). Assign projects to geographies and add
   them up, using dbt.
4. **Export and upload** (`04_export.sh`). Write CSV, shapefile and FGDB outputs
   and push them to `edm-publishing`.

Corrections get applied at a few points from reviewed CSVs. How to write,
review, and ingest them is in the
[wiki](https://github.com/NYCPlanning/data-engineering/wiki/Product:-KPDB).

### Assigning projects to geographies

Each project is matched to the boundaries it falls in, then its units are split
between them.

- Which geometry gets used depends on the project. Large developments, multi-lot
  subdivisions, and neighborhood studies are matched as polygons. Everything
  else is matched on its centroid. This gets decided once, in `kpdb_match_geom`.
- A project matched as a polygon only gets assigned to a boundary it covers at
  least 10% of.
- Units are split by how much of the project's area is in each boundary. Those
  shares are then scaled to add up to 1, so a project that lost a small piece to
  the 10% rule still has all its units assigned.
- A project that doesn't match any boundary gets assigned to one it intersects.

### Phasing

Units get put in three buckets, as a share of each project's net units: within 5
years, 5 to 10 years, and after 10 years. Some sources give us the shares. For
the rest we assume them from project status.

Source | Project type | Shares | Bucket
-- | -- | -- | --
DOB | Complete | assumed | counted as complete
DOB | Permit issued, application filed, in progress | assumed | within 5 years
DOB | Inactive (not withdrawn) | assumed | 5-10 years
DOB | Withdrawn | assumed | left out
DCP Application | all statuses except Record Closed | assumed | 5-10 years
DCP Application | Record Closed | assumed | left out
HPD RFPs | | given | within 5 years
EDC | | given | EDC's build year
Empire State Development | | planner input | one project, Atlantic Yards
Neighborhood study commitments | Downtown Far Rockaway, Inwood | given | EDC estimates
Neighborhood study commitments | Jerome, Bay Street | given | planner estimates
Neighborhood study commitments | ENY, East Harlem | assumed | within 5 years of the rezoning effective date
Neighborhood study projected sites | | assumed | spread evenly over the buildout
Future neighborhood studies | | assumed | spread evenly over the buildout

**These buckets are relative, not calendar dates.** DOB phasing follows job
status, so it moves every build. Shares that come from a source were written on
whatever date that file was made. So "within 5 years" counts from roughly
whenever the build ran, not from a fixed year.

## Outputs

| Output | What's in it |
|---|---|
| `kpdb` | one row per project record, the main product |
| `aggregation.zip` | project-level and summary tables by census tract, NTA, CDTA, and community district |
| `sca_aggregation.zip` | the same by school district, elementary school zone, and school subdistrict |
| `cpp_housing_growth/` | housing growth by NTA and community district, for the Capital Projects Portal |
| `review.zip` | tables for reviewing matches and corrections by hand |
| `summary_record_phasing` | phasing summary |

### Capital Projects Portal outputs

Two CSVs, one row per geography, with the years written into the column names:

| Column | What it is |
|---|---|
| `geography_id` | NTA or community district code |
| `units_2020_census` | 2020 Census count, as of April 1 2020 |
| `units_2020` | units as of the end of 2020: the census count plus the rest of that year |
| `completed_units_2016_2025` | net units completed over the past 10 years |
| `completed_units_2021_2025` | net units completed between `units_2020` and `units_2025` |
| `units_2025` | `units_2020` plus the line above |
| `projected_completed_units_2026_2035` | KPDB units phased within the next 10 years |
| `projected_units_2035` | `units_2025` plus the line above |

Year-labeled counts are as of the end of that year, so a project completed in
2025 counts toward 2025. The Census counts housing as of April 1 2020, so
`units_2020` adds the rest of 2020 to it and `units_2020_census` keeps the raw
number.

`units_2020_census` is the only fixed number. The rest move with the
`dcp_housing` version, and the projected window has the same caveat as phasing
above. Both `completed_units_*` columns are net of demolitions and lost units,
so they aren't gross completions.

## Running a build

Builds run through the `KPDB - Build` GitHub workflow. To run the phases
locally, with `BUILD_ENGINE_SCHEMA` set and recipe inputs loaded:

```bash
cd products/knownprojects
./bash/01_dataloading.sh   # agency files from edm-private
./bash/02_build.sh         # combine, match, deduplicate
./bash/03_aggregate.sh     # assign to geographies (dbt)
./bash/04_export.sh        # write output files
```

## Updating source data

Recipe inputs get updated in `edm-recipes` and referenced in `recipe.yml`. Check
that `dcp_housing` and the ZAP datasets cover a similar period before building.

Agency files are requested from partners, uploaded to `edm-private` under
`dcp_housing_team/db-knownprojects/`, then referenced by filename in
`python/__init__.py`:

- Empire State Development, `esd_projects`
- EDC projections and shapefile, `edc_projects` and `edc_dcp_inputs`
- Neighborhood study commitments, `dcp_n_study`
- Future and past neighborhood studies, `dcp_n_study_future` and `dcp_n_study_projected`
- HPD RFPs, `hpd_rfp`
- DCP planner-added projects, `dcp_planneradded`

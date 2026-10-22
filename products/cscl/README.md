# CSCL ETL

Source of truth documentation can be found [here](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/PROJECTS/DATA%20ENGINEERING/PROJECT%20PLANNING/LION%20ETL/ETL_V8_02012024%20-%20Copy.docx?d=wd3194825302642958a316fb030749aa1&csf=1&web=1&e=3zxoek).

This has been converted to a README in this private repo: github.com/NYCPlanning/cscl_etl_archive

## Concepts/Terminology

See [Design Doc Appendix A](./design_doc.md#appendix-a-conceptsterminology)

## Source Data

All source datasets (per version) come from the same gdb file. See any "dcp_cscl" ingest templates for an example of how to add additional layers.

## Organization of models

As other dbt projects, we have:
- `staging` models for minor tweaks to source data to "stage" them for transformation.
- `intermediate` models for actual transformation logic.
  - within, transformations are organized in subfolders by section of the ETL docs that define them.
  - any transformations that are more broad/foundational may live in the `intermediate` folder itself.
- `product` tables that are more final product/output focused.
  - subfolders for specific output/category - LION dat files, LION gdb, SEDAT outputs, etc.

There's also an `etl_dev_qa` folder: QA models that compare the build against prod's outputs. They run in the QA stage (`builds.qa` in `recipe.yml`), not the build.

## Workflow - Adding a new output

This workflow is written for the fixed-width text outputs (Geosupport inputs). GDB outputs differ in the formatting and validation steps.

Basically, for a new file output, you must
- [setup](#setup):
  - make sure we have "prod" files from GR. Prod outputs live [here](https://nyco365.sharepoint.com/:f:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL?csf=1&web=1&e=XfVWF2).
  - put said "prod" files in `edm-private/cscl-etl/{version}/`.
  - create a "dat" formatting csv in `./seeds/formatting`.
    - if this is not a "dat" file with formatting rules for fields, strict field lengths, etc, you might need to make a tweak to `./poc_validation/prod_data_loader.py`. Look at cases like "enders", "last_word" in both `recipe.yml` and how they're handled (based on file format, and formatting) in prod_data_loader.
  - add them as an export in `recipe.yml`. See others as examples. The custom formatting field should point to the formatting csv seed (and is not actually used by dcpy export utilities).
  - rebuild the prod snapshot: run the "CSCL - Prod snapshot" workflow for `{version}` with `overwrite` checked (or `python poc_validation/build_prod_snapshot.py build -v {version} --upload --overwrite`). It parses every prod output into the `production_outputs.zip` parquet bundle that the build loads (recipe input `dcp_cscl_production_outputs`) into schema `production_outputs`.
- [transform](#write-the-actual-transformations)
  - add any new source layers to `recipe.yml` if needed.
  - add staging and int tables as needed to actually perform transformations as required by the etl docs.
  - add product tables. the final product table must match the name of the entry in `recipe.yml` for this output for dcpy export utils to work. For "dat" files, typically we have both a
    - `{file}_by_field` model which calls the `apply_text_formatting_from_seed` macro to format individual fields.
    - `{file}` which calls the `select_rows_as_text` macro to create a densely-formatted text column to export.
- [validate](#validate-new-outputs): compare outputs to production. See section for more details.
- document: add sections to [design_doc.md](./design_doc.md) to summarize formatting, sources of fields, and transformation details.

### Setup

When GR has a new release, they will put files in a folder [here](https://nyco365.sharepoint.com/:f:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL?csf=1&web=1&e=XfVWF2)

**All files should be downloaded, ETL Working GDB should be zipped, then they should be uploaded to `edm-private/cscl_etl/{version}`. Norm for version here is lowercase letter.** This is where
- ingest template looks for a working gdb to ingest
- `./poc_validation/build_prod_snapshot.py` (using `prod_data_loader.py`'s parsers) looks for prod outputs to turn into the prod snapshot used for comparison.

Then build the release's prod snapshot: run the "CSCL - Prod snapshot" workflow (`cscl_prod_snapshot.yml`) with the version. It lists what it expects in `edm-private/cscl_etl/{version}/` (plus `nyclion_{version}.zip` on the DCP website) and fails if anything is missing, and it refuses to replace an existing snapshot unless `overwrite` is checked. The recipe input `dcp_cscl_production_outputs` takes the build's own version, so bumping `version` in `recipe.yml` is the only edit; a CI build for a version with no snapshot fails right after planning and says to run the workflow (`python poc_validation/build_prod_snapshot.py check-snapshot -v {version}` checks it locally).

Formatting csvs are then created. I've copied and pasted from the old etl design doc, then done some editing. When done, you should have these columns:
| header | used in formatting macro | used in prod_data_loader |
|-|-|-|
| fic | | |
| field_name | X | X |
| field_label | | |
| field_length | | X |
| start_index | | X |
| end_index | | |
| justify_and_fill | X | |
| blank_if_none | X | |

As you can see, not all are critical, but all should be included - FIC links these to the design docs, and field_label is a nice human-readable label which often comes from the design docs. All fields are from design docs, roughly, though:
- field_name is made by us - this is what the column name will be in the build (for both the table being queries within the macro and the output table)
- justify_and_fill - see our new [design doc](./design_doc.md#justification-and-fill). Default to RJSF
- blank_if_none - not always specified in design doc. Default to FALSE. Basically, if you have a zero-filled field (as in, 5 character field where `1` becomes `"00001"` instead of `"    1"`), by default a null value would become zeroes. In some cases, they should be blank-filled instead despite it being a RJZF field. In those cases, set this to true.

Some files will not need this csv at all - `last_word` for example is a csv output, but is still parsed by `prod_data_loader` by how it handles files based on format in `recipe.yml`. GDBs are compared with `poc_validation/compare_gdb.py` instead (see [Validating the district gdb](#validating-the-district-gdb)).

### Write the actual transformations

Our main guidance for this is the design doc. However, in some cases it is helpful to look at the [source code](https://github.com/NYCPlanning/cscl_etl_archive) to actually see what is implemented.

Main requirements outside of the actual business logic are
- add any new source layers to `recipe.yml` if needed (and sources.yml)
  - add staging and int tables as needed to actually perform transformations as required by the etl docs.
- the product tables. the final product table must match the name of the entry in `recipe.yml` for this output for dcpy export utils to work. For "dat" files, typically we have both a
  - `{file}_by_field` model which calls the `apply_text_formatting_from_seed` macro to format individual fields.
  - `{file}` which calls the `select_rows_as_text` macro to create a densely-formatted text column to export (and should match `recipe.yml`)

### Validate new outputs

#### Data/field validation

If theres no great unique key for an output table, or if you just want a quick pass, the following query is great
```sql
WITH combined AS (
    SELECT 
    	'dev' as source,
        *,
        md5(CAST(dev AS text)) AS row_hash
    FROM {my_schema}.{table}_by_field as dev
    UNION ALL
    SELECT 
    	'prod' as source,
        *,
        md5(CAST(prod AS text)) AS row_hash
    FROM {my_schema}.{table}_by_field as prod
),
counts AS (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY row_hash) AS match_count,
        COUNT(CASE WHEN source = 'dev' THEN 1 END) OVER (PARTITION BY row_hash) AS dev_count,
        COUNT(CASE WHEN source = 'prod' THEN 1 END) OVER (PARTITION BY row_hash) AS prod_count
    FROM combined
)
select *
FROM counts
WHERE dev_count <> prod_count
order by ...
```

With the concept of a "key" in the data though, we can make actual comparisons between specific fields of records in prod vs this pipeline. See logic in `./models/etl_dev_qa` for how this is done for LION. As a quick and dirty thing, you could edit these files to aim at your tables and run them, or compile the code and run manually.

For non-LION flat file outputs, it's fine to do this with queries as opposed to creating models like we do for LION - LION is just such a huge transformation that the ease of having these models is super useful. It would be great to have these queries in some sort of marimo notebook instead.

#### Output file validation

As a final check, you can also compare the actual output files. The QA stage (`builds qa run`, as in `cscl_build.yml`) does this: it pulls the prod files and runs `poc_validation/validate_outputs.sh`

```bash
python3 poc_validation/prod_data_loader.py pull
./poc_validation/validate_outputs.sh
```

This sorts the files before comparison, so this isn't quite valid for comparing outputs that actually have some sort of sorting requirement. However for LION and other unordered files, it currently works as a check of total records with issues in the actual output files.

## District boundary gdb

`v26B_Districts.gdb.zip` holds the 43 district polygon feature classes defined by chapter 10 / table 39 of the ETL doc. Models live in `models/product/districts/`, one per feature class, named `gdb_<feature class>`.

Feature class and field names follow **appendix D**, which is stale in places but was confirmed against the shapefiles DCP publishes on BYTES — those use appendix D's names exactly (`AssemDist`, `BoroCD`, `HCentDist`, `SchoolDist`, `StSenDist`). The `name` values in `product-metadata/products/lion/*` are friendly display names, not field names; don't build from them.

Two places where the 2009 doc and the real product disagree, both settled empirically against the published shapefiles:

- **Land is `WATER_FLAG IN ('2', '3', '4')`**, not `('2', '3')` as the doc says. Flag `4` postdates the doc and is land for clipping purposes; including it reproduces the published clipped extent exactly, excluding it falls about 1% short.
- **Table 39 understates which layers are clipped.** It says layers with no `wi` twin are "extracted exactly as stored", but `nyfb`, `nyfc`, `nyfd`, `nypp`, `nysd`, `nynta2010`, `nynta2020`, `nycdta2020` and `nypuma2010` are all shoreline-clipped in the published product. Only `nyap`, `nyhez` and the internal-only layers are genuinely unclipped.

### How the boundaries are built

District polygons are assembled from the AtomicPolygon topology, not by overlaying source
polygons: each AP edge is classified as interior or exterior to the district(s) that own
it, and the exterior edges are polygonized. Shoreline comes for free, since a land-water
edge is always exterior. See the header of `macros/district_boundary.sql` for the steps.

### Validating the district gdb

Prod archives the geodatabase alongside the flat files, so this is a direct dev/prod diff:

```bash
python3 poc_validation/compare_gdb.py
```

This script covers every gdb export in `recipe.yml` (the LION gdb and the district gdb alike), not just this one. It reports per-layer column set and row count for all of them, and additionally total area for polygon layers (per `geometry_type: polygons` in each export's `recipe.yml` entry) — that covers every district layer. Output is one CSV per gdb, e.g. `output/validation_output/v26B_Districts_comparison.csv`, plus a whitelist (`KNOWN_STRUCTURAL_DIFFS` in the script) for already-understood structural diffs like `nyura` below.

Prod quirks worth knowing before reading a diff:

- **`nyura` carries a stale copy of `nybid`'s schema** (`BIDID`/`BID`/`BOROUGH`) instead of anything urban-renewal related. Both prod and the CSCL source layer are empty, so the mistake never shows up in data. We emit the real URA identifiers, so a structural diff on this one layer is expected and is whitelisted in the comparison script.
- **`nymcea` is singlepart.** Clipping splits some MCEAs into disjoint pieces and prod writes each as its own feature, so the 115 dissolved (borough, MCEA) groups become 122 features.
- **`nymc`/`nymcwi` exclude `MUNICOURT = '00'`**, an unassigned placeholder on 6 of the 34 source rows. That's what takes them to 28 features.
- **`nypuma2020` ships in prod** even though table 39 predates it.

### Known dev/prod diffs

Accounted-for district diffs are listed in [data_issues.md](./data_issues.md) under
`CSCL-DISTRICTS-*`.

## LION Differences File (LDF)

The LDF documents what changed between the previous LION release and this one. It doesn't
fit the "adding a new output" workflow above, for three reasons: it takes **two** LION
releases as input, it's the only output carrying **state across releases**, and it's the
only one GR builds with a separate tool rather than the ETL tool.

Two files, both 100-byte fixed width: `LDFBASE.dat` (records) and `LDFHEADER.dat` (one
header record).

### Record types and where each comes from

| Type | Content | Source |
|---|---|---|
| `H` | Header: both release IDs and dates, record count, cumulative number | Assembled at export |
| `N` | Node added / deleted / moved | Diff of previous vs current LION |
| `S` | Base centerline segment change | `CENTERLINEHISTORY` |
| `P` | Physical segment change | `CENTERLINEHISTORY` |
| `G` | Generic segment change | `CENTERLINEHISTORY` |

Records are emitted in the order **N, S, G, P** — not alphabetical, and not the order the
2009 spec implies.

> [!IMPORTANT]
> Use the layout in `CSCL III LDF` (the 2008 Phase III doc), **not** the 2009 DCP `LDF.pdf`.
> The PDF is superseded: it describes only three record types, 6-character dates, and
> populated LION-key fields. The real files have five record types, 8-character `MMDDYYYY`
> dates, and the LION-key positions (18–27 and 51–60) as permanent filler. Every field
> offset in `seeds/text_formatting/text_formatting__ldf_*.csv` was verified byte-for-byte
> against GR's published 26a and 26b files.

### `CENTERLINEHISTORY` is the LDF table

Despite the name, `CENTERLINEHISTORY` is the "LDF table" of the Phase III design — the
rename described in that doc never happened. It's CSCL's edit journal: every centerline
add, delete, merge, split and node change, with the lineage that a set comparison of two
LION releases cannot recover (a split looks like one delete plus two adds).

**Rows for the release being cut carry a NULL `release_num`.** GR stamps it at publish
time, which is after the GDB snapshot we ingest is taken. Select on NULL — never on
`release_num = '<this release>'`, which matches nothing.

### Transitory record elimination — the open gap

`int__ldf_segments` drops the lineage of segments that were created *and* destroyed between
two releases. Our rule is an approximation of GR's, which lives in an assembly we don't
have, so this output does not match prod exactly.

**The dev/prod difference is CSCL-LDF-01 in [data_issues.md](./data_issues.md)**; its bead
has why it exists and what constrains fixing it, alongside the other open data questions
for this output (CSCL-LDF-*). Read that before changing the elimination logic; in
particular, don't tune it to match prod's counts on the two editions we have.

### Cumulative record numbers

Every record carries a number in positions 91–100 that is cumulative *across editions*, so
a value identifies one record in one edition forever. It chains exactly:

```
26a header: cumulative 565223 + count 3611 = 568834 = 26b header cumulative
```

GR's tool takes this number as operator input; ours derives it from the previous edition's
header instead. That difference matters — see
CSCL-LDF-03 in [data_issues.md](./data_issues.md).

### Release inputs

Four header fields aren't in the source data: the two release IDs and the dates each was
deployed. GR's tool prompts an operator for all four. We record them as plain rows in
`seeds/config.csv` (see that seed's description in `seeds.yml`):

```
key,value
ldf_previous_version,26a
ldf_old_release_date,2026-02-09
ldf_new_release,26b
ldf_new_release_date,2026-04-22
```

`int__ldf_header.sql` reads them straight from the seed (`config_value('ldf_previous_version')`
etc., no dbt `--vars` flag needed) and errors at build time if any of the four rows is missing,
instead of emitting a header with wrong dates. `ldf_previous_version` also picks the LION
release the node records diff against, and `ldf_new_release`/`ldf_new_release_date` describe
*this* release - keep `ldf_new_release` in sync with `recipe.yml`'s top-level `version` by hand
when it's bumped, since nothing ties them together automatically.

### Validating

GR archives their own `LDF.dat` and `LDF.header` in `edm-private/cscl_etl/<version>/`, so
this output has direct ground truth — unlike most others, no separate prod pull is needed.

The prior release's citywide LION and LDF header - what `int__ldf_nodes.sql`/
`int__ldf_segments.sql` diff against, and where `int__ldf_header.sql` gets the cumulative
record number this edition continues from - both come in as normal recipe sources now
(`recipe.yml`'s `dcp_cscl_temp_mock_previous_release`, imported as `previous_citywide_lion`
and `previous_ldf_header`), not ad-hoc loads. Both are pinned, hand-parsed CSVs (see that
recipe entry's comment) - bump them by hand alongside `seeds/config.csv`'s
`ldf_previous_version` when the release this LDF documents moves on; nothing currently keeps
the three in sync automatically.

Prod's own LDF for this release is part of the prod snapshot (see [Setup](#setup)), which
`qa__ldf_diffs` and `qa__ldf_summary` compare against.

## LION - Known data issues

Individual LION discrepancies — what's accepted, what's still open — live in
[data_issues.md](./data_issues.md) under `CSCL-LION-*`. What follows is the runbook for a
new build, not a list of issues.

### Common things to check for in new build

Things that have gone wrong on a new release before. In general, most issues are discovered/handled by looking into `qa__lion_dat_individual_diffs`, but there are a few special things

- duplicate boro + face_code + segmentid in dev. This is diagnosed by looking at `qa__lion_dat_summary` and looking at "unique keys in dev". This is a source data issue to be reported to GR. GR's discrepancy log (linked from [data_issues.md](./data_issues.md)) has some info on when this happened before
- duplicate boro + face_code + segment_seqnum in prod. This should only happen in cases of the above. Needs a custom query. This has no impact on our pipeline but GR wants to know.
- empty geoms. This has happened in the past. We probably want to add more data testing to `sources.yml`. But also a simple query of certain layers/tables where `ST_ISEMPTY(geom)` will work too.
- sectionalmap issues. This would appear in `qa__lion_dat_summary` in terms of number of dev rows and `qa__lion_dat_individual_diffs` with `sectional_map` diffs (and other diffs as well due to duplicate rows). Topology issues in the source data have caused duplicate rows on joins.
- giant circles. Open up int__primary_segments in qgis and see if we have a large hadron collider in nyc.

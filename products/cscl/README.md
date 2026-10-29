# CSCL ETL

Source of truth documentation can be found [here](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/PROJECTS/DATA%20ENGINEERING/PROJECT%20PLANNING/LION%20ETL/ETL_V8_02012024%20-%20Copy.docx?d=wd3194825302642958a316fb030749aa1&csf=1&web=1&e=3zxoek).

This has been converted to a README in this private repo: github.com/NYCPlanning/cscl_etl_archive

Running a release? See the [Release runbook](#release-runbook) at the end of this file. For how the build works end to end, see [docs/architecture.md](./docs/architecture.md).

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
  - create a "dat" formatting csv in `./seeds/text_formatting`.
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
- document: describe the model and its columns in its yml and add it to `doc_config.yml`, so it appears in the generated export doc (see [CONTRIBUTING.md](./CONTRIBUTING.md#documentation-generation)).

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

If there's no good unique key for an output table, or you just want a quick pass, this query compares whole rows of the build's table and prod's (from the prod snapshot, schema `production_outputs`). The two must have the same columns in the same order.
```sql
WITH combined AS (
    SELECT
        'dev' AS source,
        *,
        md5(CAST(dev AS VARCHAR)) AS row_hash
    FROM {my_schema}.{table}_by_field AS dev
    UNION ALL
    SELECT
        'prod' AS source,
        *,
        md5(CAST(prod AS VARCHAR)) AS row_hash
    FROM production_outputs.{prod_table} AS prod
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

The line comparison sorts both files first, so it counts records that differ but can't see order. Record order is checked separately, per file, by `poc_validation/file_order.py`: every flat file has to be in prod's record order (`order_rule` in [`seeds/lion_outputs.csv`](./seeds/lion_outputs.csv); GSS's loader picks values by record position). It matches the two files' records by full line and reports:

- `order_rows_compared`: records both files have (the rest are the line diff's business),
- `rows_out_of_order`: the fewest of those that would have to move to put ours in prod's order (the common records minus the longest run of them already in prod's order: the longest increasing subsequence), 0 when the order matches,
- `order_matches_prod`, and the first departure: the first place, walking both files' common records in step, where the next record differs, as a line number on each side (`first_order_departure_prod_line`, `_dev_line`).

The numbers go into `validation_summary.csv`, the lines themselves into `file_order.csv`. A LION borough file takes under a second. The export doc's Record order section, generated from `lion_outputs.csv`'s `prod_order`, `spec_order` and `order_differs_from_spec`, says where prod's order departs from the ETL spec.

## District boundary gdb

`v<V>_Districts.gdb.zip` holds the 43 district polygon feature classes defined by chapter 10 / table 39 of the ETL doc. Models live in `models/product/districts/`, one per feature class, named `gdb_<feature class>`.

Feature class and field names follow **appendix D**, which is stale in places but was confirmed against the shapefiles DCP publishes on BYTES — those use appendix D's names exactly (`AssemDist`, `BoroCD`, `HCentDist`, `SchoolDist`, `StSenDist`). The `name` values in `product-metadata/products/lion/*` are friendly display names, not field names; don't build from them.

38 of the layers also ship as shapefiles (`dataset_files/shapefiles/<layer>.zip`, BYTES and Open Data), written by packaging from the same linear parquet layers. Each gdb entry names its shapefile file in `custom.product_metadata.shapefile`. Field names are the layer's, cut to 10 characters the way ArcGIS exports an FGDB layer (`CENSUSBLOCK_2000_SUFFIX` -> `CENSUSBL_1`, `SHAPE_Length` -> `Shape_Leng`, shape fields last). That matches every prod 26c shapefile's names and order. Field widths don't match prod: ours are GDAL's defaults (String 80, Integer 9, Real 24.15), prod's are String(gdb width), Integer(5) and Real(19.11).

Two places where the 2009 doc and the real product disagree, both settled empirically against the published shapefiles:

- **Land is `WATER_FLAG IN ('2', '3', '4')`**, not `('2', '3')` as the doc says. Flag `4` postdates the doc and is land for clipping purposes; including it reproduces the published clipped extent exactly, excluding it falls about 1% short.
- **Table 39 understates which layers are clipped.** It says layers with no `wi` twin are "extracted exactly as stored", but `nyfb`, `nyfc`, `nyfd`, `nypp`, `nysd`, `nynta2010`, `nynta2020`, `nycdta2020` and `nypuma2010` are all shoreline-clipped in the published product. Only `nyap`, `nyhez` and the layers published as stored (`nybid`, `nyhd`, `nyura`, `nyzip`) are unclipped.

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

Accounted-for district diffs are the `CSCL-DISTRICTS-*` rows of
[seeds/known_differences.csv](./seeds/known_differences.csv).

## LION Differences File (LDF)

The LDF documents what changed between the previous LION release and this one. It doesn't
fit the "adding a new output" workflow above, for three reasons: it takes **two** LION
releases as input, it's the only output carrying **state across releases**, and it's the
only one GR builds with a separate tool rather than the ETL tool.

Two files, both 100-byte fixed width: `LDF.dat` (records) and `LDF.header` (one header
record). These are the names prod's files have; the ETL spec calls them `LDFBASE.dat` and
`LDFHEADER.dat`.

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

### Transitory record elimination

`int__ldf_segments` drops journal records that were undone or superseded before the
release. It is a port of prod's rule, `CSCL_Editor.LDFExtractHelper` in `CSCLEditor.dll`
(shipped in the prod ETL archive in edm-private), written as a dbt Python model because the
rule is sequential: rows are flagged in journal order and later decisions read earlier
flags. Method names in the model follow prod's.

It deviates from prod in one place, **CSCL-LDF-01 in [seeds/known_differences.csv](./seeds/known_differences.csv)**:
prod drops a G or P record when its generic or physical ID is deleted anywhere in the
journal, even on another segment, which loses reassignments LION shows took effect. Ours
only counts a delete on the same node pair. Everything else should match prod record for
record; a new difference is a porting bug, so compare against the decompiled source before
changing the logic.

### Cumulative record numbers

Every record carries a number in positions 91–100 that is cumulative *across editions*, so
a value identifies one record in one edition forever. It chains exactly:

```
26a header: cumulative 565223 + count 3611 = 568834 = 26b header cumulative
```

GR's tool takes this number as operator input; ours derives it from the previous edition's
header instead. That difference matters — see
CSCL-LDF-03 in [seeds/known_differences.csv](./seeds/known_differences.csv).

### Release inputs

Four header fields aren't in the source data: the two release IDs and the dates each was
deployed. GR's tool prompts an operator for all four. We record them as plain rows in
`seeds/config.csv` (see that seed's description in `seeds.yml`):

```
key,value
ldf_previous_version,26b
ldf_old_release_date,2026-04-22
ldf_new_release,26c
ldf_new_release_date,2026-07-27
```

(The seed's `description` column is left out here.)

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
[seeds/known_differences.csv](./seeds/known_differences.csv) under `CSCL-LION-*`. What follows is the runbook for a
new build, not a list of issues.

### Common things to check for in new build

Things that have gone wrong on a new release before. In general, most issues are discovered/handled by looking into `qa__lion_dat_individual_diffs`, but there are a few special things

- duplicate boro + face_code + segmentid in dev. This is diagnosed by looking at `qa__lion_dat_summary` and looking at "unique keys in dev". This is a source data issue to be reported to GR. GR's [discrepancy log](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL/DE%20Pipeline%20-%20Project%20Tracking/Data%20Discrepancy%20Tracking/LION%20Flat%20Files%20%E2%80%93%20Data%20DiscrepancyIssue%20Tracking.docx?d=w60907e50f8044bd9bffe2508a299035f&csf=1&web=1&e=aZ59n8) has some info on when this happened before
- duplicate boro + face_code + segment_seqnum in prod. This should only happen in cases of the above. Needs a custom query. This has no impact on our pipeline but GR wants to know.
- empty geoms. This has happened in the past. We probably want to add more data testing to `sources.yml`. But also a simple query of certain layers/tables where `ST_ISEMPTY(geom)` will work too.
- sectionalmap issues. This would appear in `qa__lion_dat_summary` in terms of number of dev rows and `qa__lion_dat_individual_diffs` with `sectional_map` diffs (and other diffs as well due to duplicate rows). Topology issues in the source data have caused duplicate rows on joins.
- giant circles. Open up int__primary_segments in qgis and see if we have a large hadron collider in nyc.

## QA sign-off

Three outside teams sign off on the outputs before the new ETL replaces the old one:
GR (Geographic Research, upstream: the source of the CSCL data), GSS (GeoSupport
Services, downstream: Geosupport reads the flat files) and GIS (DCP's GIS team, Esri
tooling). [`seeds/signoff_matrix.csv`](./seeds/signoff_matrix.csv) says who signs off on
what: flat files GR + GSS, the District gdb (and the shapefiles made from it) GR + GIS,
the LION gdb GR. A reviewer the matrix doesn't require for an output is `n_a` everywhere
for it, and the seed tests enforce that.

Sign-off is tracked at three levels:

1. **Known difference**, in [`seeds/known_differences.csv`](./seeds/known_differences.csv):
   `gr_status`, `gss_status`, `gis_status`, each `not_reviewed`, `presented`, `agreed`,
   `disputed` or `n_a`, plus `reviewed_on` and `review_ref`. These are the reviewers'
   positions. `status` and `decision` stay our own call: `accepted` means we're not
   changing it, not that anyone outside agreed.
2. **Row**, in `qa__diffs_all`: `accounted_for` means a known difference explains the
   row; `approved` means every reviewer the file's output group needs has agreed to every
   known difference the row cites. `diffs_report.csv` counts unexplained
   (`unaccounted_discrepant_rows`), explained but awaiting approval, and approved rows per
   file. A row that's accounted for but cites no known difference can't be approved
   (`qa__diffs_all` warns on these).
3. **Output file**, in [`seeds/lion_outputs.csv`](./seeds/lion_outputs.csv):
   `gr_signoff`, `gss_signoff`, `gis_signoff` (`pending`, `approved`, `rejected` or
   `n_a`), plus `signoff_date` and `signoff_ref`. This is the reviewer signing off on the
   file as a whole, once its rows are explained and its known differences agreed.

To record a review, edit the seed in a PR: set the status, set `reviewed_on` (or
`signoff_date`) to the date it changed and `review_ref` (or `signoff_ref`) to the meeting
(date and who was there) or a link to the email or issue. `assert_signoff_recorded` fails
on a status past `not_reviewed`/`pending` without both. The export doc's known-differences
tables show each reviewer's status.

All statuses were reset to `not_reviewed`/`pending` on 2026-10-28, when review with GR
and GSS started fresh; the earlier `gr_status` values (`confirmed`/`pending`) are in git
history.

## Legacy bug fixes behind flags

When we find a prod bug that downstream teams (GR, GSS) read through, the fix goes behind a
dbt var in `dbt_project.yml`, off by default, so the build reproduces prod until they agree
to roll it out. Each such var has a comment there: what it fixes, the prod code, and its
known-difference id. The known difference stays in
[`seeds/known_differences.csv`](./seeds/known_differences.csv), and its `qa__diffs_*` rule
accounts for the difference only with the fix on: with it off, a diff on that field is
unaccounted, since the build should match prod. Every build logs what the fix would change,
whether it is on or not, so it isn't lost.

| var | default | fixes | logged as |
|---|---|---|---|
| `fix_boe_lgc_pointer` | `false` | CSCL-LION-04: prod writes BOE LGC pointer 1 whenever the BOE-preferred LGC is above 9 (LION L10, LION gdb `BOE_LGC`) | CSCL-LOG-OURS-06: a count in `log.csv` (saying whether the fix is on), records in `log_boe_lgc_pointer.csv` |
| `fix_lion_lr_from_topology` | `false` | CSCL-LION-16: prod takes a segment's left/right AtomicPolygon from a probe 2 ft either side of its midpoint, which overshoots into the AtomicPolygon behind where the adjacent one is shallow; on, they come from the AtomicPolygon topology edges the segment runs along (LION .dat and gdb: AtomicPolygon, census, districts, zip, locational status, ...) | CSCL-LOG-OURS-08: a count of LION records and segments in `log.csv` (saying whether it is on), records in `log_lion_lr_topology.csv` |
| `fix_single_arc_curve_flag` | `false` | CSCL-LION-03: prod leaves the curve flag blank on a non-centerline segment that is one circular arc end to end (LION L49, LION gdb `CurveFlag`) | CSCL-LOG-OURS-07: a count in `log.csv` (saying whether the fix is on) |
| `fix_nonstreet_proto_zip` | `false` | CSCL-LION-02: prod leaves both zips blank on a proto-segment of a shoreline or non-street feature off the borough boundary (LION L24/L33, LION gdb `LZip`/`RZip`) | CSCL-LOG-OURS-09: a count in `log.csv` (saying whether the fix is on) |

Turn one on for a build with `--vars`, e.g. `dbt run --selector duckdb_build --vars
'{fix_boe_lgc_pointer: true}'` (pass the same `--vars` to the QA models' run, or their rules
won't match the build), or set it in `dbt_project.yml` once the reviewers have agreed
(record that on the known difference, see [QA sign-off](#qa-sign-off)). A new flag follows
the same pattern: the var with its comment, the correct and prod values side by side in the
model and a unit test for each setting, a `CSCL-LOG-OURS-*` log row, and the QA rule gated on
the var.

## Release runbook

How to run a CSCL release (next up: 26D) from GR's delivery to a reviewed build. It's
written for someone who hasn't done one before, and it'll change over the next few
releases. `<v>` is the release in lowercase (`26d`), `<V>` the same in uppercase (`26D`).
Run local commands from `products/cscl` with direnv loaded (see
[docs/development.md](../../docs/development.md)).

A release has two inputs, and both have to be in place before you build:

- **The source**: CSCL's ETL Working GDB for this release, ingested into `edm-recipes`.
- **Prod**: GR's legacy-pipeline outputs for the same release, which QA compares against.

### 1. Preconditions

1. **GR delivers the legacy outputs** (see [Setup](#setup)). Everything for the release goes
   in `edm-private/cscl_etl/<v>/`: the flat files, the LDF, and the zipped ETL Working
   GDB. The prod snapshot also downloads `nyclion_<v>.zip` from the DCP website, so that
   has to be published too.
2. **Ingest the source GDB** (`ingest_templates/dcp_cscl_gdb.yml`). The template reads
   `edm-private/qa_app/inbox/dcp_cscl_gdb/<v>/ETL Working GDB.gdb.zip`. That's the QA
   app's inbox, not `cscl_etl/<v>/`, so upload the zip there too. Then either:
   - use the QA app's **Inbox Ingest** page (`dcp_cscl_gdb`, version `<v>`), **checking
     "Tag as latest"**, which is off by default; or
   - run the **Ingest Single Dataset** workflow (`ingest_single.yml`) with dataset
     `dcp_cscl_gdb`, version `<v>`, and `latest` left checked.

   The tag matters because the recipe doesn't pin the GDB: it loads whatever
   `edm-recipes/raw/dcp_cscl_gdb/latest/` points to. Raw versions are named by ingest
   timestamp (the latest so far is `2026-09-17T14:26:25...`). The per-layer datasets
   (`edm-recipes/datasets/dcp_cscl_centerline/<v>/` etc.) carry the clean version. The QA
   app's **CSCL Builds** page lists `<v>` once those exist.
3. **Product metadata**: bump `current_version_lion` in
   [`product-metadata/snippets/strings.yml`](../../product-metadata/snippets/strings.yml)
   (it's `26c` today). The lion datasets' `current_version` and their BYTES links and
   descriptions read it. The build itself doesn't read it.

### 2. Prod snapshot

Run the **"CSCL - Prod snapshot"** workflow
([`cscl_prod_snapshot.yml`](../../.github/workflows/cscl_prod_snapshot.yml)) with `version`
set to `<v>`:

- `version`: lowercase, as in `edm-private/cscl_etl/`.
- `overwrite`: leave it off. It refuses to replace an existing snapshot unless this is
  checked. Turn it on only to rebuild a snapshot on purpose (e.g. after adding a new output).
- `dev_image`: off, unless you're testing a branch image.

"Check legacy outputs" (`build_prod_snapshot.py check-inputs`) lists what's expected in
`cscl_etl/<v>/` (plus the nyclion web zip) next to what it found, and fails if anything is
missing. "Build and upload" then parses every output into parquet (~96 tables) and uploads
`edm-private/datasets/dcp_cscl_production_outputs/<v>/production_outputs.zip`.

**Good**: the run is green, and the summary shows `production_outputs.zip` and its table
count. Locally, `python poc_validation/build_prod_snapshot.py check-snapshot -v <v>`
checks that it exists.

### 3. Version bump

Make a branch and edit `recipe.yml`:

- **The top-level `version:`** (`26c` -> `26d`). Everything else follows it: the prod
  snapshot input (`dcp_cscl_production_outputs`, `version_env_var: VERSION`) and every
  export filename that embeds the release (`nyclion_{{ version }}.zip` x4,
  `v{{ version | upper }}_Districts.gdb.zip` x43). Those names have to match prod's for the
  QA comparisons to line up. `build.yml`'s `version` input overrides it for a one-off build.

Nothing ties the LDF inputs to the version, so update these by hand too (see
[Release inputs](#release-inputs)):

- `seeds/config.csv`: `ldf_previous_version` (-> the release before, `26c`),
  `ldf_old_release_date` (its deploy date), `ldf_new_release` (`26d`, must equal `version`),
  `ldf_new_release_date` (this release's deploy date, from GR).
- `recipe.yml`'s two `dcp_cscl_temp_mock_previous_release` inputs (`version: 26B` today),
  which should become the previous release's. Only `26B/` exists in
  `edm-private/datasets/dcp_cscl_temp_mock_previous_release/` right now (see the open
  questions below).

### 4. Build

Run the **"🏗️ Build a Dataset"** workflow
([`build.yml`](../../.github/workflows/build.yml)) on your branch with `dataset_name: cscl`.
It calls [`cscl_build.yml`](../../.github/workflows/cscl_build.yml). You can also use the
QA app's **CSCL Builds** page, which dispatches the same workflow with a generated name.

```bash
gh workflow run build.yml --ref <branch> -f dataset_name=cscl \
  -f build_name=cscl_26d_first_build -f build_note="26d first build"
```

- **`build_name`** becomes the upload folder `db-cscl/build/<build_name>/`. It defaults to
  the branch name, and `db-cscl/build/main/` is the baseline every branch build's summary is
  compared against, so give a release build its own name. Start it with a letter and use
  only letters, digits and `_`. The CSCL Builds page uses
  `cscl_<v>_<note-slug>_<YYYYMMDDTHHMM>`.
- **`build_note`**: plain words only. A `#`, `:` or parentheses breaks the Health Check job's
  input parsing, and the run fails within seconds.

The stages, with times from a recent run (about 27 min in total):

| Step | What it does | Time |
|-|-|-|
| Plan build | `builds plan` -> `recipe.lock.yml` (resolves `latest` input versions) | <1 min |
| Check prod snapshot | fails here, with instructions, if step 2 wasn't run for this version | <1 min |
| Dataloading | sources into DuckDB (curved layers as linear tables + `<table>_arcs`) | ~2 min |
| Build | the recipe's `builds.build` commands: dbt deps, seed, run, test | ~3 min |
| Export | parquet-first: flat files plus one GeoParquet per FGDB layer | <1 min |
| Package | FGDBs (re-curved, metadata stamped), zips, generated docs | ~4 min |
| QA | the recipe's `builds.qa` commands (see step 5) | ~15 min |
| Upload | package + `validation_output/` to `edm-publishing` `db-cscl/build/<build_name>/` | ~1 min |

**Good**: every step is green. The step summary has the timings,
`validation_summary.csv`, the diffs report table (with deltas against main) and the
source freshness report. Check `attachments/source_data_versions.csv` in the build
folder: `dcp_cscl_gdb` should be the timestamp of your step 1 ingest, and
`dcp_cscl_production_outputs` should be `<v>`.

### 5. Reading QA

Everything lands in `edm-publishing` under `db-cscl/build/<build_name>/validation_output/`
(also attached to the run as the `cscl-duckdb-logs-and-qa` artifact). The QA app's **CSCL
QA** page reads it, including the per-file diff rows.

- **`diffs_report.csv`**: one row per output in `seeds/lion_outputs.csv`. Read
  `unaccounted_discrepant_rows` first: those diffs aren't explained by a known difference.
  `accounted_for_discrepant_rows` are explained ones, split into
  `awaiting_approval_discrepant_rows` and `approved_discrepant_rows` (see
  [QA sign-off](#qa-sign-off)), and
  `discrepant_rows_from_file_comparison` is the whole-line file diff (sorted lines, so it
  ignores row order). Order is in `rows_out_of_order` and the other order columns (see
  [Output file validation](#output-file-validation)); `out_of_prod_order` is true for a
  file that has to be in prod's order and isn't, and the step summary counts those. `status`/`status_summary` are the curated state from
  `lion_outputs.csv`, and the `*_signoff` columns the file's reviewer sign-offs. For scale, the 26c build on main on 2026-10-21 had 29,685 diff rows,
  571 unaccounted, and 62 of 99 files exact. A new release should land near that; a big
  jump in unaccounted rows for one file is what to chase.
- **`qa__diffs_all_summary.csv`**: the field-level counts behind the report, by
  `output_file_id`. The rows themselves are in `qa__diffs_*` models in the build DuckDB.
- **`source_freshness.csv`** (and `source_freshness.md` in the step summary): the max
  created/modified dates of the main source layers, and evidence that prod was built from a
  newer or older extract than ours (e.g. prod street codes we don't have, above our highest
  code in their block of 100). It never fails the build. A warning means some diffs are
  probably source vintage, not our bugs; prod 26c mixed vintages. If it warns, ask GR which
  extract they used before chasing those diffs.
- **FGDB metadata drift check**: runs first in `builds qa run` (also
  `python -m dcpy lifecycle builds qa gdb-metadata -r recipe.lock.yml`). For every gdb
  export with `custom.product_metadata`, the layer's stamped Esri metadata attrs must equal
  its fields, and it **fails the QA step** if they don't. Fix it in `product-metadata/` or
  the model, whichever is wrong.
- **`district_review.gdb.zip`**: every district layer next to prod's, for review by another
  team. For each layer `<L>` there's `<L>_new` (ours, with metrics and `class`: `identical`,
  `grid_noise`, `minor`, `major`, `new_only`, `prior_only`, `duplicate_key`), `<L>_prior`
  (prod's, for the non-identical ones), `<L>_diff` (the symmetric-difference pieces above
  grid noise), plus the `summary` and `summary_by_layer` tables. The 26c baseline had 55
  major, 29 minor and 1 duplicate_key out of ~243k districts (26c, 2026-10-27).
  `duplicate_key`: the key is on a different number of features on each side
  (`n_features_new`, `n_features_prior`), so the review doesn't compare them (nyct2010wi
  4990100: prod has three water tracts on that key, we have one).
- **Known differences**: [`seeds/known_differences.csv`](./seeds/known_differences.csv) is
  the record of accepted and open diffs vs prod (stable IDs like `CSCL-LION-04`), cited by
  the `qa__diffs_*` models that account for them. After a release, update `last_verified`
  on the rows you re-checked, remove rows whose difference is gone (retire the ID), and
  add rows for anything new
  (see [CONTRIBUTING.md](./CONTRIBUTING.md#handling-discrepancies)).
- **Build log (`log.csv`)**: [`seeds/log_messages.csv`](./seeds/log_messages.csv) lists
  every message prod's ETL logs (ids like `CSCL-LOG-SAF-25`), plus the checks only we log
  (`CSCL-LOG-OURS-NN`), with whether we log it yet (`status`) and which `log__*` model
  does. Each `log.csv` row carries its id in `log_message_id` and at the start of its
  message, so it lines up with prod's log; `qa__log_message_counts` counts rows per id.
  When you add a check, give it a seed row (or flip a `planned` one to `implemented`) and
  emit the id as the model's `log_message_id`, with the wording from the `log_message`
  macro; `log.sql` adds the seed's `effect` and the `[id] ` prefix, and
  `assert_log_message_citations` keeps the models and the seed in step.

### 6. When something is off

- **Reproduce locally.** It's the same commands as CI, so it's the same stages:
  ```bash
  python3 -m dcpy lifecycle builds plan recipe --recipe-path ./recipe.yml --output-path recipe.lock.yml
  python3 -m dcpy lifecycle builds load load --recipe-path recipe.lock.yml
  python3 -m dcpy lifecycle builds build run --recipe-path recipe.lock.yml
  python3 -m dcpy lifecycle builds export export --recipe-path recipe.lock.yml
  python3 -m dcpy lifecycle builds export package --recipe-path recipe.lock.yml
  python3 -m dcpy lifecycle builds qa run --recipe-path recipe.lock.yml
  ```
  To iterate on single models afterwards, point dbt at the build's DuckDB
  (`export DUCKDB_PATH=$(python3 -m dcpy lifecycle builds build path --recipe-path recipe.lock.yml --duckdb)`,
  plus `BUILD_ENGINE_SCHEMA` from direnv) and run e.g.
  `dbt run --select qa__diffs_lion_dat+`. Set `DUCKDB_MEMORY_LIMIT`/`DUCKDB_THREADS` for
  your machine (see `profiles.yml`).
- **Find the rows.** Start from the file's `qa__diffs_*` model, or its member in
  `diff_rows.zip`. [Common things to check for in new build](#common-things-to-check-for-in-new-build)
  lists past failure modes (duplicate keys, empty geometries, sectional map, giant circles).
- **Decide whose it is.** If it's ours, fix it. If it's prod's, or source vintage, add a
  `known_differences.csv` row and cite it from the `qa__diffs_*` model. If it's legacy
  behaviour we can't infer from the code or the docs, ask GR.
- **Asking GR.** Collect the questions, each with the specific keys/rows and what you
  checked, and send them as a batch. Record the answer in the seed's `gr_status` (see
  [QA sign-off](#qa-sign-off)) and `decision`. Data errors in CSCL itself (e.g. duplicate segment keys) go to GR as well
  (see GR's discrepancy log, linked under "Common things to check for in new build").

### 7. Publishing and distribution

What exists today:

- **Drafts**: `db-cscl/draft/26B/` and `db-cscl/draft/26C/` exist in `edm-publishing`,
  promoted from builds by hand. `db-cscl` isn't an option in the **Promote a Dataset to
  Draft** workflow (`promote_to_draft.yml`), so it's done with the CLI:
  `python3 -m dcpy.connectors.edm.publishing promote_to_draft --product db-cscl --build <build_name> --draft-summary <note>`.
  The CSCL Builds page says a successful build is promoted to draft automatically, but
  `cscl_build.yml` has no promote step: it only uploads to `build/`.
- **`db-cscl/draft_for_gss/26C/`** holds two hand-made zips (`26C_0923_flatfiles.zip`,
  `26C_0923_full.zip`, 2026-09-23). They look like a hand-off, but nothing in the repo
  writes them.
- **Publish/package**: `db-cscl` isn't an option in `publish.yml` or `package.yml`.
  Nothing publishes our CSCL build today.
- **Open Data**: LION datasets (`product-metadata/products/lion/*`, `socrata` destinations)
  are distributed from **BYTES**, i.e. GR's legacy release on the DCP website, via the
  **Distribute Datasets** workflow (`distribute_socrata_from_bytes.yml`, `source: bytes`).
  The **Bytes Quarterly Updates** ingest (`ingest_bytes_quarterly.yml`) archives the BYTES
  release into `edm-recipes`. Neither touches our build.

Open questions (not answered by anything in the repo):

- When our build replaces GR's release, where do the outputs go? Geosupport inputs, BYTES,
  Open Data? And who promotes and publishes: GR or us?
- What is `draft_for_gss` (who's GSS, and what do they expect in those two zips)? Is it a
  per-release step?
- Should `db-cscl` be added to `promote_to_draft.yml` (and `publish.yml`), or should
  `cscl_build.yml` promote to draft itself, as the CSCL Builds page assumes?
- `dcp_cscl_temp_mock_previous_release`: how were the `26B` CSVs
  (`citywide_lion_dat.csv`, `previous_ldf_header.csv`) made, and who makes `26C`'s for 26d?
- `seeds/config.csv`'s `thined_version`/`thined_file_tag`: what's the update rule per
  release?
- Release dates for the LDF header (`ldf_new_release_date`): GR gives us these, but at what
  point in the release?

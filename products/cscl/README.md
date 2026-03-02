# CSCL ETL

Source of truth documentation can be found [here](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/PROJECTS/DATA%20ENGINEERING/PROJECT%20PLANNING/LION%20ETL/ETL_V8_02012024%20-%20Copy.docx?d=wd3194825302642958a316fb030749aa1&csf=1&web=1&e=3zxoek).

This has been converted to a README in this private repo: github.com/NYCPlanning/cscl_etl_archive

## Concepts/Terminology

See [Design Doc Appendix A](./design_doc.md#appendix-a-conceptsterminology)

## Source Data

All source datasets (per version) come from the same gdb file. See any "dcp_cscl" ingest templates for an example of how to add additional layers - 25a and 25b raw files are currently archived, so you can create a new template and run it for 25b without issue.

## Organization of models

As other dbt projects, we have:
- `staging` models for minor tweaks to source data to "stage" them for transformation.
- `intermediate` models for actual transformation logic.
  - within, transformations are currently organized in subfolders by section of the ETL docs that define them.
  - any transformations that are more broad/foundational may live in the `intermediate` folder itself.
- `product` tables that are more final product/output focused.
  - subfolders for specific output/category - LION dat files, LION gdb, SEDAT outputs, etc.

There's also a `etl_dev_qa` folder. These maybe in a more formal approach would be run separately from the build step. They can be dropped after this pipeline is put into operation

## Workflow - Adding a new output

For now, this is a little all focused around densely-formatted text outputs (geosupport inputs). A slightly different workflow will be needed for gdb outputs. Have fun.

Basically, for a new file output, you must
- [setup](#setup):
  - make sure we have "prod" files from GR. Prod outputs live [here](https://nyco365.sharepoint.com/:f:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL?csf=1&web=1&e=XfVWF2).
  - put said "prod" files in `edm-private/cscl-etl/{version}/`.
  - create a "dat" formatting csv in `./seeds/formatting`.
    - if this is not a "dat" file with formatting rules for fields, strict field lengths, etc, you might need to make a tweak to `./poc_validation/prod_data_loader.py`. Look at cases like "enders", "last_word" in both `recipe.yml` and how they're handled (based on file format, and formatting) in prod_data_loader.
  - add them as an export in `recipe.yml`. See others as examples. The custom formatting field should point to the formatting csv seed (and is not actually used by dcpy export utilities).
  - load production data into `db-cscl.production_outputs` by running `python3 poc_validation/prod_data_loader.py -v {version} -d {dataset_name}`.
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
- logic in `./poc_validation/prod_data_loader.py` looks for prod outputs to load into the build database for comparison. See 

You could point `prod_data_loader` to look at a folder on your comp, but by default it will pull files locally from edm-private, then load them into the database.

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

Some files will not need this csv at all - `last_word` for example is a csv output, but is still loaded into the db using `prod_data_loader` by how it handles files based on format in `recipe.yml`. gdbs are not yet supported. Again, have fun!

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

With the concept of a "key" in the data though, we can make actual comparisons between specific fields of records in prod vs this pipeline. See logic in `./models/etl_qa_dev` for how this is done for LION. As a quick and dirty thing, you could edit these files to aim at your tables and run them, or compile the code and run manually.

For non-LION flat file outputs, it's fine to do this with queries as opposed to creating models like we do for LION - LION is just such a huge transformation that the ease of having these models is super useful. It would be great to have these queries in some sort of marimo notebook instead.

#### Output file validation

As a final check, you can also compare the actual output files. If you don't have the prod outputs on your computer, you can do this like it's done in the github action `cscl_build.yml`

```bash
python3 poc_validation/prod_data_loader.py pull
./poc_validation/validate_outputs.sh
```

This currently sorts the files before comparison, so this isn't quite valid for comparing outputs that actually have some sort of sorting requirement. However for LION and other unordered files, it currently works as a check of total records with issues in the actual output files.

## LION - Known data issues

As we've continue to validate the outputs for LION, we've come across a bunch of "known" issues that have either been resolved but may return, or we've accepted that there are diffs, or what have you. A lot of these are documented loosely [here](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL/DE%20Pipeline%20-%20Project%20Tracking/Data%20Discrepancy%20Tracking/LION%20Flat%20Files%20%E2%80%93%20Data%20DiscrepancyIssue%20Tracking.docx?d=w60907e50f8044bd9bffe2508a299035f&csf=1&web=1&e=aZ59n8)

### Common things to check for in new build

As in, we're doing 26a for the first time. Some things that could go wrong. In general, most issues are discovered/handled by looking into `qa__lion_dat_individual_diffs`, but there are a few special things

- duplicate boro + face_code + segmentid in dev. This is diagnosed by looking at `qa__lion_dat_summary` and looking at "unique keys in dev". This is a source data issue to be reported to GR. See link above, there's some info on when this happened before
- duplicate boro + face_code + segment_seqnum in prod. This should only happen in cases of the above, though I'm not 100% sure. Needs a custom query. This has no impact on our pipeline but GR wants to know.
- empty geoms. This has happened in the past. We probably want to add more data testing to `sources.yml`. But also a simple query of certain layers/tables where `ST_ISEMPTY(geom)` will work too.
- sectionalmap issues. This would appear in `qa__lion_dat_summary` in terms of number of dev rows and `qa__lion_dat_individual_diffs` with `sectional_map` diffs (and other diffs as well due to duplicate rows). I think this is fully resolved but there were topology issues in the source data causing duplicate rows on joins.
- giant circles. Open up int__primary_segments in qgis and see if we have a large hadron collider in nyc.


### Doubly-reversed proto segments

| lionkey | segmentid | 
|-|-|
| 3966000330 | 0016558 | 
| 2866000025 | 0343093 | 
| 2866000020 | 0343094 | 
| 2865900235 | 0343095 | 
| 2865900230 | 0343096 |

These segments are reversed protosegments. In prod, they are not reversed. This is because of a bug in the prod etl where
- geometry-modeled segments are processed on at a time.
- during this, protosegments for a given geometry-modeled segment are looked up and processed.
- while a protosegment is processed, it refers back to the fields of the geometry-modeled segment
- if a protosegment is reversed, it flips many fields in its representation of the source segment.
- If there are multiple reversed protosegments for a single geometry-modeled segment, the same fields get flipped back and forth erroneously.

GR confirmed this is a bug, and we will not try to recreate.

### 10 rows missing zip code 10035 in production

The following segmentids are in the middle of Randall's island, which is all the same zip code. They're all missing zip in production.
- 0246013
- 0246014
- 0246016
- 0246017
- 0246018
- 0246019
- 0246021
- 0246022
- 0279055
- 0279056 

GR has said this is fine.

### Curve Flag

Many rows have `curve_flag` = 'I' while prod has blank. These are compoundcurves, which in our pipeline get coerced to multistring for geometric operations while prod handles some other way, and how they check if a curve is "irregular" doesn't work for them.

GR has confirmed this is fine. Specifically, for diffs where `field = 'curve_flag' AND 'dev' = 'I' AND prod = ' ' AND source_table <> 'centerline'`

### BOE LGC Pointer

This is wrong for 568 records. This is an error at the Face Code level, it applies to 5 face codes.

GR has confirmed ours is right.

### Nonstreet Feature segment sequence numbers

These just don't match prod for funky reasons, since they're generated on the fly. They're only generated for nonstreet feature segments, and they're only used as a unique key in LION, so it's fine that they don't match as long as they're unique.

GR has confirmed this is fine.

### Coincident segments

There are some remaining, and some decision needs to be made with GR as to how we handle this

### Center of curvature

I had resolved all of these for 25d by "linearizing" geoms with a very small tolerance. Looks like we've got at least one back in 26a. Can chat with GR if we want to play whack-a-mole or if the difference is small enough that we're okay

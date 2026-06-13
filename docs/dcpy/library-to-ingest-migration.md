# Library → Ingest Migration

TLDR:
- run `python3 -m dcpy lifecycle scripts validate_ingest convert {ds}` to create a template scaffold in `dcpy/lifecycle/ingest/templates`
- complete the generated template to best of your abilities
- run `python3 -m dcpy lifecycle scripts validate_ingest run_and_compare {ds}`
- iterate
  - tweak template/code as needed
  - run `python3 -m dcpy lifecycle scripts validate_ingest run_single ingest {ds}`
  - run `python3 -m dcpy lifecycle scripts validate_ingest compare {ds} (-k {key_column})`
- add `columns` field
  - use `python3 -m dcpy lifecycle scripts validate_ingest get_columns {ds}` to get all columns and types
  - search codebase for table references (or use judgement) and add columns that we depend on to the `columns` field of template
- run `python3 -m dcpy lifecycle scripts validate_ingest run_single ingest {ds}` to make sure final template is valid
- run `python3 -m dcpy lifecycle scripts validate_ingest compare {ds} (-k {key_column})` and copy-paste output into tracking issue

# Context

We are migrating from `dcpy.library` to `dcpy.lifecycle.ingest` as our tool of choice for ingesting (extracting, some amount of minimal processing/transforming, then archiving) source datasets to edm-recipes, our bucket that's used as a long term data store.

This migration is being done on a dataset-by-dataset basis. Our [github actions](../../.github/workflows/ingest_single.yml) that previously targeted `library` or `ingest` now hit `dcpy.lifecycle.scripts.ingest_with_library_fallback`, which runs `ingest` to archive a dataset if a template for it exists in `dcpy/lifecycle/ingest/templates`, and falls back to trying to use `library` if it doesn't. This cli target is `python3 -m dcpy lifecycle scripts ingest_or_library_archive { dataset } { ... }`.

In terms of how datasets are being prioritized, see the [project managing the migration](https://github.com/NYCPlanning/data-engineering/issues/1255).

# Migrating a dataset
## Writing the new template
**First step - check if the template is in `dcpy/lifecycle/ingest/dev_templates`!** These aren't finished, as code has moved and they haven't been maintained, but they'd be good starting points over the library template.

Easiest thing to do is run the command

```bash
python3 -m dcpy lifecycle scripts validate_ingest convert {ds}
```

This creates a bit of a scaffold for the new template, with the copied in library template beneath it for reference. Schema for the template can be found [`dcpy/models/lifecycle/ingest.py`](../../dcpy/models/lifecycle/ingest.py) (and json schema [here](https://nyc3.digitaloceanspaces.com/edm-publishing/data-engineering-devops/schemas/ingest/template.json) in s3), though it's just as if not more helpful to look at the other examples in the `templates` folder. Do take some care to determine the `type` of the source - if we have a link to s3, make it an s3 source, not a url. If that s3 link was just us dumping it in `edm-recipes/inbox`, we can actually just make it a local file now, since `ingest` archives the raw dataset - no need for `inbox`.

### `columns` field
The templates have a `columns` field, where we can list columns with both `id` and `data_type`. Columns are tough to define when starting, since you haven't looked at any of the data yet - leave them blank. Once the new template has been validated, there's a CLI target - `get_columns` that will print out copy and paste-able yml to dump into the template. You should definitely run ingest again after adding the columns field to ensure that all fields are valid.

With a (mostly) finished template, it's time to validate and iterate as needed.

## Requirements
You'll be running ingest and library via command line. gdal has breaking changes in 3.9.x to library, so if you have a later version installed locally, you'll either need to downgrade, or run in a docker container (such as the dev container). I have moved away from working inside the dev container, so for this, I built the dev container but did not have vs code run within it, and simply prefix commands with `docker exec de ...` (the container is named "de" now) to run the commands below in the running container.

## One-liner to run and compare
`python3 -m dcpy lifecycle scripts validate_ingest run_and_compare dcp_specialpurpose`

Likely, you will need to do more than just run this one command

## Running ingest/library
The code to run both tools lives mostly in [`dcpy/lifecycle/scripts/ingest_validation.py`](../../dcpy/lifecycle/scripts/ingest_validation.py), and it's targets from here that are hit. That file contains logic to
1. run the tool(s) - library and ingest - without pushing to s3, just keeping file outputs local
2. load to the database `sandbox` in `edm-data` (in schema based on your env)
3. run utilities from `dcpy.data.compare` to compare the two

The first two points are bundled into a (few) single command(s)
- `python3 -m dcpy lifecycle scripts validate_ingest run dcp_specialpurpose`
- `python3 -m dcpy lifecycle scripts validate_ingest run_single library dcp_specialpurpose`
- `python3 -m dcpy lifecycle scripts validate_ingest run_single ingest dcp_specialpurpose -v 20241001`

The first runs both library and ingest, the second/third is a target to just run one of them. Typically, you'll start with the first (or the one-liner to run and compare from up above), and then re-run ingest as needed. Version can be supplied as an option to either command.

## Comparing outputs
The code to compare lives in [`dcpy/data/compare.py`](../../dcpy/data/compare.py), though we still access it only through the lifecycle script we've already been using.
- `python3 -m dcpy lifecycle scripts validate_ingest compare dcp_specialpurpose`

This has a couple options, all of which you will likely use. With no options, it returns a `SqlReport` object that compares
- row count
- columns/schema
- data - simple comparison (`select * from left except select * from right` essentially).

Your first, option-free command might return empty dataframes for the data comparison. If so, great! We're done. But more likely we have to make changes. But before iterating on the template, based on what we see it might be helpful to run with some options first.

### `-c` option - column/schema-only comparison
With `-c` specified, it skips the data comparison. This is sometimes helpful (or necessary) as a first pass.

If you got a sql error instead of a report, this is for you. The only error I've gotten is from invalid data comparison - str vs date, str vs int, etc. Running with `-c`, the print out the nrows and column name/type comparison will give you enough info to hopefully "fix" the ingest template. Likely, you'll need to add `clean_column_names` step and `coerce_column_types`.

### `-k` option - key columns
With `-k` option, you can specify key column(s) to make the data comparison more informative

It's tough to just compare rows. But maybe now looking at the output, or querying the tables in dbeaver, you can see a little bit of structure - this table looks like it's one row per `bbl`, or `sdlbl`, or what have you. Great - we can run compare now with the `-k` option to specify a `key` for this table, so we can compare by key. I.e., for bbl '1000010001', what values have changed? It's a much more informative comparison.

If you have a compound key (say, `boro`, `block`, and `lot`), you can specify multiple - `python3 -m dcpy lifecycle scripts validate_ingest compare dcp_zoningtaxlots -k bbl -k boro -k block -k lot` is what was needed to identify unique keys in ztl and then make comparisons. This keyed report both summarizes any keys present in one table but not the other, then also compares other columns for each key.

Hopefully at this point, you have some sort of informative comparison. So the data is different - what do we do?

## Iterating - types of changes needed
There's one blanket change that we've decided is acceptable. `character varying` columns are now being created as `text`.

Beyond that, you might need to do a variety of things. Particularly with geometry differences, you might need to query the database, and determine if the differences need changing, and how. You will need to do one or two things to tweak the data

### Add processing steps
#### Old default steps
Library by default
- always lowercased column names and replaced spaces with underscores
- generally coerced polygons to multipolygons

v0 of ingest had these steps baked in, but they're being removed from the code of ingest, meaning they likely will need to be defined in your template.
```yml
  processing_steps:
  - name: clean_column_names
    args: {"replace": {" ": "_"}, "lower": True}
  - name: multi
```
These aren't run by default for a couple reasons
- if they're not needed for a specific dataset, it's nice to run less steps
- not ALL geospatial datasets are actually coerced to `multi` by library
- when adding new datasets, we might prefer a difference scheme of cleaning names (and certainly would prefer to not automatically turn polygons to multi)

However, they are both probably needed for many datasets. For multi, a quick search of our codebase shows that several transformations rely on logic along the lines of `WHERE st_geometrytype(geom) = 'MultiPolygon'`. Unless you're certain that this dataset is fine, in general convert polygons to multi. It seems more common that we did not coerce to multi in the case of points (dcp_addresspoints, for one).

#### other processing steps
For shapefiles, you'll likely need
```yml
  - name: rename_columns
    args:
      map: {"geom": "wkb_geometry"}
```
A necessary evil. May we one day rid ourselves of the column "wkb_geometry". Today is not that day.

There are a variety of steps

### code changes to ingest
There's a chance you'll want to preprocess in some way, and that function doesn't exist! For example, for dcp_dcmstreetcenterline, the dataset actually is of POINTZ geometry - 3D points. However, after running library and ingest and querying the database, I found that all z values were zero. So both to make identical with library (and as it turned out without losing any actual data), the dates needed to be coerced to 2d. There's actually a gpd geoseries function to do just that, and we had a preprocessor to call a generic **pd Series** function, so [I extended it](https://github.com/NYCPlanning/data-engineering/pull/1254/commits/8481c7ec1e793a9be615081cfc57fdfa7c03e4ce) to also allow for GeoSeries function calls as well.

Processing steps should be as generic and parameterizable as possible.

If you need to write a new processing function, you also need to write a test for it. Thems the rules.

## Acceptance criteria
The report doesn't give a binary yes/no or pass/fail to our new dataset. How do we make that decision?

### Column/Schema changes
Relatively simple here, as we just have two types of changes
- new/missing columns - all columns and their names must match. There's an argument that this could be limited to columns we use, and I wouldn't add a new processing step just for some column we'll never reference. But if a column can be fixed with a preprocessing step that's already being called, let's include it
- data type changes. different text types is the exception. But no numeric to text or vice-versa, or datetime to text, etc.

### Data changes
There are several definite failures
- new/missing rows
- for keyed comparison, differing keys present
- for numeric columns, inequality other than small (< 0.01% let's say) rounding errors
- for text, inequality other than handling of newlines

### Data changes - geometry
Geometry MUST be spatially equal. But equality goes beyond that. Ideally, we have complete equality. Before adding a `make_valid` call within ingest, I was seeing cases where ordering of polygons within a multi polygon are different. This is fine.

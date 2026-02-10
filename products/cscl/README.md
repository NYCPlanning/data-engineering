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

## Workflow - Adding a new output
For now, this is a little all focused around densely-formatted text outputs (geosupport inputs). A slightly different workflow will be needed for gdb outputs. Have fun.

Prod outputs live [here](https://nyco365.sharepoint.com/:f:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL?csf=1&web=1&e=XfVWF2).

Any new output needs to be validated against prod outputs generated from the same source version gdb (25a, 25b, etc). For text files, we can take a similar approach to the LION flat (.dat) files.


### Setup
When GR has a new release, they will put files in a folder in the above sharepoint location

**All files should be downloaded, ETL Working GDB should be zipped, then they should be uploaded to `edm-private/cscl_etl/{version}`. Norm for version here is lowercase letter.** This is where
- ingest template looks for a working gdb to ingest
- logic in `./poc_validation/dat_loader` looks for prod outputs to load into the build database for comparison. See 

### Data Validation Step 1: formatting


1. Downloading production outputs from sharepoint into a folder named `prod`.
2. Running the full pipeline (or, if doing this piecemeal, running `bash/export.sh`) to have outputs in a folder named `output`.
3. Running `poc_validation/validate_outputs.sh`. This is a little specific to the .dat outputs, but could be either tweaked as a new script to handle other outputs or be more generalized. For 2 files being compared, what this does is:
   - sort each file.
   - run a pairwise comparison between the files.
   - prints all rows present in one file and not the other (and all NOT in the first but in the second).
   This captures some broad strokes differences. Technically, it can't really differentiate between "duplicates/missing" rows and rows that just have differences, but that's the scope of this comparison, it's not intended to get into details about what the differences are, just see how close the outputs are. Also, if order matters in the outputs, this will currently ignore that since it sorts both outputs.

### Validating specific rows/fields based on key
If the above validation works (and 100% match), this may not be needed (like potentially for SEDAT outputs...).

With the concept of a "key" in the data, we can make actual comparisons between specific fields of records in prod vs this pipeline. Again, what's been done so far is fairly specific to the LION dat outputs, but a similar approach could be taken and some code re-used. For comparisons to be made, the following steps must be taken:

1. Downloading production outputs from sharepoint into a folder named `prod`
2. Loading this data into the database. This is done through `poc_validation/prod_data_loader.py`. This file
   - reads in `seeds/text_formatting/text_formatting__lion_dat.csv`, which has the formatting rules for all the lion .dat columns
   - uses that to "slice" up the formatted text in the `prod` output files to convert the text into columns in a pandas dataframe
   - loads this dataframe into the database
3. Running the pipeline at least through a full dbt build.
4. Running manual sql comparisons... OR once you've settled on something adding a dbt model. See `models/poc_dat_comparison.sql` for example (both as just a sql example and how to make it a model). For a simple output where we reach 100% accuracy quickly, we probably don't need a dbt model. But you'll need to figure out how to join the two tables and actually compare data.

Different outputs will have different unique keys for rows, the docs should specify.

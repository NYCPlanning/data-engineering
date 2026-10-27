# CSCL ETL - Contributor Guide

## Project Overview

CSCL (Citywide Street Centerline) is a **data engineering project** (Python + DBT) to replace a legacy C# ETL system maintained by NYC Planning's Geographic Research (GR) team.

### What This Pipeline Does

**Input:** File Geodatabase (FGDB) extracted from OTI's CSCL Oracle database

**Output:** Package of flat files and FGDB that feed into DCP's Geosupport System
- LION flat files (DAT format) - street segment data for geocoding
- Lookup tables - supporting reference data
- File geodatabases - public datasets

### The Legacy System

The original C# application extracted files from an FGDB using **now-deprecated ESRI tooling**. This system is documented in:
1. **Legacy C# source code** - [github.com/NYCPlanning/cscl_etl_archive](https://github.com/NYCPlanning/cscl_etl_archive) (private repo)
2. **Original documentation** - Converted from Word doc to markdown in the archive repo
3. **[design_doc.md](./design_doc.md)** - Modernized version documenting the new pipeline

**When in doubt: consult the code first, then the markdown documentation.**

## Project Status & Goals

This project is **in progress**. Our goal is to **reproduce legacy outputs as closely as possible**.

### Output Validation Strategy

Each quarter, we receive a set of outputs from the legacy system that serve as our "gold standard":
- We compare our pipeline outputs against these quarterly legacy outputs
- We work to match them record-by-record, field-by-field

### Handling Discrepancies

**When outputs don't match:**
1. **If we find bugs in the legacy system** → Fix them in our code and **document the discrepant records**
2. **If we can't reproduce legacy behavior** → Document why and note the differences
3. **All known discrepancies must be tracked** - each gets a stable ID, as a row in [seeds/known_differences.csv](./seeds/known_differences.csv), cited from the `qa__diffs_*` model that accounts for it. The export doc lists each output's rows. When a difference no longer shows up in the `qa__diffs_*` models, remove its row and retire the ID (see the seed's description in [seeds/seeds.yml](./seeds/seeds.yml))

### Project Status Tracking

`seeds/lion_outputs.csv` is the backbone status record for every output file/gdb layer, not
just comparison config - it's what the CI build's step summary reports from. Each row's
`status` and one-sentence `status_summary` are the at-a-glance state; `notes` backs that up
with the fuller explanation (citing a known-difference ID for anything substantial). If an output is blocked on something outside this team - e.g.
needing NYC Planning's GR team's input on undocumented legacy behavior - record that there
(`status: GR Review` while it's with them, `blocked` once it's a known dead end) rather than
leaving it untracked.

### Adding a log check (chaos first)

Every log.csv message has an id in [seeds/log_messages.csv](./seeds/log_messages.csv). A new
log check starts as a test that fails:

1. Add a mutation for its id to `MUTATIONS` in
   [poc_validation/chaos_monkey.py](./poc_validation/chaos_monkey.py): an edit to the loaded
   inputs that should make the message fire, listing every id it should trigger. Update the
   seed's `chaos_mutations` column to match (`tests/test_chaos_monkey.py` checks it).
2. Run the loop below and watch the id show `expected fail` in `qa__chaos_summary`.
3. Write the `log__*` model, set the seed row to `implemented`, and rerun until the id
   shows `pass`: every injected record logged under the id (`logged = injected`), dropped
   records really gone from their output (`not_dropped = 0`), and no new log rows beyond
   the injected records (`unexpected_rows = 0`).

The loop works off a baseline: a copy of an inputs-loaded DuckDB with a full build on it
(the `duckdb_build` selector, ~2 minutes), made once. Each run (~1 minute) copies it,
rebuilds the models that differ from the code the baseline was built from
(`state:modified+`, so the log it compares against is the current code's), injects,
rebuilds the models between the edited input tables and the chaos QA models
(`source:<table>+,+qa__chaos_summary`), and prints
`qa__chaos_summary`. The rest of the copy stays as the baseline built it:

```bash
cp inputs.duckdb built.duckdb
DUCKDB_PATH=built.duckdb python poc_validation/chaos_monkey.py --build-baseline   # once
python poc_validation/chaos_monkey.py --run --baseline built.duckdb --seed 1
# --ids CSCL-LOG-X-NN to restrict, --max-per-check k, --work <dir> for the copy
```

The copy goes to `chaos/` next to the baseline, under the same file name: DuckDB names the
catalog after the file, and the built views refer to their tables through it, so don't
rename a built baseline either.

The baseline keeps the dbt manifest it was built from in `built.duckdb.state/`, so code
changes don't make it stale; rebuild it when the inputs change, or when the branch has moved
far enough that the catch-up step rebuilds most of the project. The fail run works off the
same baseline: check out main's models (`git checkout main -- models seeds macros`), rerun
with the same seed, then `git checkout HEAD -- models seeds macros`. Edits to
`create_duckdb_functions` aren't caught by the state comparison: rebuild the baseline after
those.

Without `--run`, `chaos_monkey.py --seed 1` injects into `DUCKDB_PATH` and prints the
`dbt run -s ...` command that rebuilds what it touched; `--report` prints `qa__chaos_summary`.

`qa__chaos_injections` has one row per injected record and expected id, with the old and new
values, for working out why one wasn't logged. Both QA models read `log_records`: log.csv
plus the records of its attachments. A check that fires thousands of times on clean data
(CSCL-LOG-FC-01, -03) puts one count row per borough in log.csv and its records in an
attachment export (`log_face_code_missing.csv`; CSCL-LOG-BYTES-06 does the same with
`log_altnames_sname_cut.csv`); the name-table warnings
(CSCL-LOG-SAF-19, -24 and the like) put one count row per name in log.csv and their
records in `log_name_table_warnings.csv`. Add such an attachment to `log_records`.

## Documentation Generation

We're replacing `docs/ETL_V8_02012024.md` (a static, hand-maintained Word-doc export)
with documentation generated from the dbt project itself - model/column `description`s
live in each model's own yml, close to the code that implements them, and
`scripts/generate_docs.py` assembles them into `output/docs/cscl_exports.md`.

**Reusable machinery** lives in `dcpy-utils` (`dcpy.utils.dbt_project`,
`dcpy.utils.doc`, `dcpy.utils.doc_config`) - parsing the dbt project, a small
format-agnostic `Doc`/`Section`/element model, and a declarative `DocConfig`. Anything
CSCL-specific (DAT field-layout tables built from `text_formatting__*` seeds, image
path conventions) lives in `scripts/generate_docs.py` and `doc_config.yml` instead -
see the module docstrings for the full design.

### `doc_config.yml`

The single source of truth for what appears and in what order - model groups and
boilerplate includes, interleaved however they're declared. A model belongs in the
generated doc because it's listed here (and tagged `config: {tags: [export_doc]}` in
its own dbt yml - `generate_docs.py` warns if the two disagree), not because a script
inferred it. See the comments at the top of `doc_config.yml` for the full directive
vocabulary (`table`, `images`, `elements`).

### Writing conventions

- **No model names.** See the RULE comment at the top of `doc_config.yml` - a dbt
  model name must never appear anywhere in the generated doc.
- **No positional cross-references.** Don't write "the generic version above" or "the
  roadbed file below" - `doc_config.yml`'s `items` order isn't fixed, and reordering it
  silently turns a correct reference into a wrong one. Link to the target section's
  anchor instead (e.g. `[GenericABCEGNPX.txt](#genericabcegnpxtxt-saf)`) - see the
  matching RULE comment in `doc_config.yml` for how to find the anchor.
- **Spell out acronyms.** Every `glossary.csv` entry for an acronym should give the
  full term in the `term` column, e.g. `Local Group Code (LGC)`, not just `LGC` -
  verify the expansion against a real source (the legacy ETL doc, Geosupport's own UPG
  glossary) rather than guessing.

### `doc_generation_plan.csv`

The work tracker - one row per model or boilerplate section still needing docs,
covering every section of the legacy ETL doc. Columns: `id`, `dbt_model`,
`type` (`model_dat_fields` / `model_columns` / `boilerplate` / `group_intro`),
`doc_config_group`, `legacy_section` + `legacy_line` (for looking up the source text),
`status`, `notes`.

**Status vocabulary:**

| Status | Meaning |
|---|---|
| `not_started` | Nothing written yet. |
| `drafted` | Description(s) written, based on the code - step 1 below done. |
| `reconciled` | Compared against the legacy doc; any real discrepancy logged - step 2 done. |
| `reviewed` | You've signed off. Only you set this status. |
| `skip` | Deliberately not carrying this forward (e.g. a deprecated legacy output, or content with no equivalent in this pipeline) - tracked so it reads as a decision, not an oversight. |

### The process, per row

1. **Generate fresh.** Write the model's `description` and column `description`s from
   your own understanding of the code (the SQL, any comments, `seeds/known_differences.csv`) - not
   copied from the legacy doc's prose. Match its level of detail (methodology, edge
   cases, value meanings), not its wording or tone.
2. **Reconcile against the legacy doc.** Read the corresponding legacy section
   (`legacy_line` in the CSV) and compare. Anything worth calling out - a gap in what
   you wrote, a rule the legacy doc states more precisely, a discrepancy between the
   two - goes in [`docs/doc_reconciliation_log.md`](./docs/doc_reconciliation_log.md).
   If a finding turns out to be a real *behavioral* discrepancy (not just a wording
   gap), it belongs in `seeds/known_differences.csv` instead - link to it from the reconciliation
   log rather than duplicating the analysis there.
3. **Review.** Flag the row for review (PR, or however you're tracking it day to day).
   Only mark `reviewed` in the CSV once you've actually looked at it - that status is
   the sign-off gate, not something to set for yourself.

Run `python scripts/generate_docs.py` (needs `target/manifest.json` - run `dbt parse`
first) to regenerate `output/docs/cscl_exports.md` and see the result; it's a build
artifact, not checked in.

## Getting Started

### Prerequisites

This product uses **direnv** for environment configuration. See [docs/development.md](../../docs/development.md#loading-environment-variables-direnv).

### Key Files

- **[README.md](./README.md)** - Detailed operational guide for adding outputs, validation workflows, and known issues
- **[design_doc.md](./design_doc.md)** - Technical specifications and business rules
- **[recipe.yml](./recipe.yml)** - Product configuration defining sources and outputs
- **seeds/text_formatting/** - DAT file formatting specifications (field lengths, justification, etc.)

### Workflow Overview

See [README.md](./README.md) for the complete workflow. In brief:

1. **Setup** - Download quarterly outputs from GR, build the prod snapshot
2. **Transform** - Implement business logic in DBT models (staging → intermediate → product)
3. **Validate** - Compare outputs to production using SQL queries and file diffs
4. **Document** - Describe the models and columns in their yml (they generate the export doc; see [Documentation Generation](#documentation-generation))

## Project Structure

```
products/cscl/
├── CONTRIBUTING.md          # This file - contributor orientation
├── README.md                # Operational guide
├── design_doc.md            # Technical specifications
├── recipe.yml               # Product configuration
├── dbt_project.yml          # DBT configuration
├── doc_config.yml          # What the generated export doc contains
├── models/
│   ├── staging/            # Minor source data tweaks
│   ├── intermediate/       # Transformation logic (organized by ETL doc section)
│   ├── product/            # Final outputs (LION, SEDAT, etc.)
│   ├── log_files/          # log.csv and its attachment
│   ├── qa_int/             # QA helpers (QA stage)
│   └── etl_dev_qa/         # QA models comparing the build to prod (QA stage)
├── macros/                 # Formatting, district boundaries, DuckDB functions
├── seeds/
│   ├── text_formatting/    # DAT file field specifications
│   ├── known_differences.csv
│   └── log_messages.csv
├── poc_validation/         # Prod snapshot, output comparisons, chaos monkey
├── scripts/                # Doc generation and seed helpers
├── tests/                  # dbt data tests and Python tests
└── docs/                   # Architecture, images and reference materials
```

## Resources

### Documentation Priority

1. **Legacy C# code** - [github.com/NYCPlanning/cscl_etl_archive](https://github.com/NYCPlanning/cscl_etl_archive) - Most authoritative
2. **[design_doc.md](./design_doc.md)** - Current pipeline specifications
3. **Original Word documentation** - In archive repo (converted to markdown)

### Quarterly Production Outputs

Located on SharePoint: [CSCL ETL Folder](https://nyco365.sharepoint.com/:f:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL?csf=1&web=1&e=XfVWF2)

### Issue Tracking

- **Data discrepancies**: [LION Data Discrepancy Tracking (Word)](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL/DE%20Pipeline%20-%20Project%20Tracking/Data%20Discrepancy%20Tracking/LION%20Flat%20Files%20%E2%80%93%20Data%20DiscrepancyIssue%20Tracking.docx?d=w60907e50f8044bd9bffe2508a299035f&csf=1&web=1&e=aZ59n8)
- **Known differences**: [seeds/known_differences.csv](./seeds/known_differences.csv)

## Key Concepts

See [design_doc.md Appendix A](./design_doc.md#appendix-a-conceptsterminology) for detailed terminology.

**Quick reference:**
- **LION** - Linear Integrated Ordered Network (street segment file for Geosupport)
- **Face Code** - Unique identifier for street segments
- **Segment ID** - Another key identifier in CSCL
- **Proto-segments** - Preliminary street segments before final processing
- **Geometry-modeled segments** - Segments with actual geometric representation
- **DAT files** - Fixed-width text format used by Geosupport

## Questions?

- **Operational workflows**: See [README.md](./README.md)
- **Business rules**: See [design_doc.md](./design_doc.md)
- **Legacy behavior**: Check [cscl_etl_archive](https://github.com/NYCPlanning/cscl_etl_archive)
- **Architecture**: See [docs/architecture.md](./docs/architecture.md)

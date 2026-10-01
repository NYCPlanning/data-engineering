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
3. **All known discrepancies must be tracked** - see [Known Data Issues](#known-data-issues) in [README.md](./README.md)

### Project Status Tracking

`seeds/lion_outputs.csv` is the backbone status record for every output file/gdb layer, not
just comparison config - it's what the CI build's step summary reports from. Each row's
`status` and one-sentence `status_summary` are the at-a-glance state; `notes` backs that up
with the fuller explanation (pointing to a `docs/prod_bugs/` writeup or a `data_issues.md`
entry for anything substantial). If an output is blocked on something outside this team - e.g.
needing NYC Planning's GR team's input on undocumented legacy behavior - record that there
(`status: GR Review` while it's with them, `blocked` once it's a known dead end) rather than
leaving it untracked.

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
   your own understanding of the code (the SQL, any comments, `data_issues.md`) - not
   copied from the legacy doc's prose. Match its level of detail (methodology, edge
   cases, value meanings), not its wording or tone.
2. **Reconcile against the legacy doc.** Read the corresponding legacy section
   (`legacy_line` in the CSV) and compare. Anything worth calling out - a gap in what
   you wrote, a rule the legacy doc states more precisely, a discrepancy between the
   two - goes in [`docs/doc_reconciliation_log.md`](./docs/doc_reconciliation_log.md).
   If a finding turns out to be a real *behavioral* discrepancy (not just a wording
   gap), it belongs in `data_issues.md` instead - link to it from the reconciliation
   log rather than duplicating the analysis there.
3. **Review.** Flag the row for review (PR, or however you're tracking it day to day).
   Only mark `reviewed` in the CSV once you've actually looked at it - that status is
   the sign-off gate, not something to set for yourself.

Run `python scripts/generate_docs.py` (needs `target/manifest.json` - run `dbt parse`
first) to regenerate `output/docs/cscl_exports.md` and see the result; it's a build
artifact, not checked in.

## Getting Started

### Prerequisites

This product uses **direnv** for environment configuration. See [AGENTS.md](../../AGENTS.md#environment-setup) for setup instructions.

### Key Files

- **[README.md](./README.md)** - Detailed operational guide for adding outputs, validation workflows, and known issues
- **[design_doc.md](./design_doc.md)** - Technical specifications and business rules
- **[recipe.yml](./recipe.yml)** - Product configuration defining sources and outputs
- **seeds/formatting/** - DAT file formatting specifications (field lengths, justification, etc.)

### Workflow Overview

See [README.md](./README.md) for the complete workflow. In brief:

1. **Setup** - Download quarterly outputs from GR, load into comparison database
2. **Transform** - Implement business logic in DBT models (staging → intermediate → product)
3. **Validate** - Compare outputs to production using SQL queries and file diffs
4. **Document** - Add transformation details to design_doc.md

## Project Structure

```
products/cscl/
├── CONTRIBUTING.md          # This file - contributor orientation
├── README.md                # Operational guide
├── design_doc.md            # Technical specifications
├── recipe.yml               # Product configuration
├── dbt_project.yml          # DBT configuration
├── models/
│   ├── staging/            # Minor source data tweaks
│   ├── intermediate/       # Transformation logic (organized by ETL doc section)
│   ├── product/            # Final outputs (LION, SEDAT, etc.)
│   └── etl_dev_qa/         # Validation queries (temporary during development)
├── seeds/
│   └── formatting/         # DAT file field specifications
├── poc_validation/         # Output comparison scripts
└── docs/                   # Images and reference materials
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
- **Known issues**: See [README.md - LION Known Data Issues](./README.md#lion---known-data-issues)

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
- **Architecture**: See [design_doc.md - Architecture](./design_doc.md#architecture)

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

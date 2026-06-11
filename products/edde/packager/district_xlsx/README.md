# District XLSX Generator

Generates XLSX files for all EDDE geographies (boroughs, community districts, citywide) from the resolved JSON data.

## Overview

This step generates Excel files that can be distributed to stakeholders. Each XLSX file contains 5 sheets:
- Demographic Conditions
- Household Economic Security
- Housing Affordability, Quality, and Security
- Housing Production
- Quality of Life and Access to Opportunity

## Output

Files are saved to `package/district_xlsx/`:
- `Bronx.xlsx`, `Brooklyn.xlsx`, `Manhattan.xlsx`, `Queens.xlsx`, `SI.xlsx` - Borough files
- `citywide_nyc.xlsx` - Citywide aggregation
- `district_XXXX.xlsx` - Community district files (55 total)

Total: 61 XLSX files generated

## Usage

Run as part of the full packager:
```bash
python3 -m packager
```

Or run standalone:
```bash
python3 -m packager.district_xlsx
```

## Implementation Notes

### Data Source
- Reads from `package/resolved_pages_and_tables/districts/*.json`
- These JSON files contain pre-formatted tables with vintages, headers, and data

### Formatting
- Matches the format of the previously VBA-generated XLSX files
- Applies proper styling: fonts, colors, borders, number formatting
- Organizes tables by race/ethnicity subcategories (Total, Asian, Black, Hispanic, White)

### Known Limitations

1. **Quality of Life sheet is empty**: The resolved JSON for qlao category has no tables. This is an upstream data issue in the packager (step 3).

2. **Some housing tables have duplicate data**: Tables 3.13 (Housing Lottery Applications) and 3.14 (Housing Lottery Leases) show the same totals across all race/ethnicity subcategories instead of breakdowns. This is also an upstream data issue.

3. **Title capitalization**: Some table titles have different capitalization than the original VBA output due to how they're stored in the source JSON. Data is identical.

## Future Improvements

- Fix upstream data issues for qlao tables
- Investigate housing lottery data duplication
- Add title normalization if exact string matching is required

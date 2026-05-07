# CSCL Known Bugs

This directory documents known bugs in the legacy CSCL ETL that have been identified through diff analysis between the legacy and new dbt-based implementations.

## Naming Convention

Bug files should follow the pattern:

```
{bug-number}-{short-description}.md
```

Where:
- `{bug-number}` is a zero-padded 3-digit number (001, 002, etc.)
- `{short-description}` is a kebab-case brief description of the bug

## Bug Documentation Template

Each bug document should include:

- **Status:** Fixed/Under Investigation/Won't Fix
- **Affected Output:** Which output files are affected
- **Severity:** Low/Medium/High
- **Discrepancy Count:** How many records show this issue
- **Summary:** Brief description
- **Technical Details:** Detailed explanation with code references
- **Example:** Concrete example with real data
- **Root Cause:** Why the bug exists
- **New ETL Implementation:** How the new ETL fixes it
- **Impact Assessment:** Analysis of the effect
- **References:** Links to relevant documentation

## Marking Diffs as Accounted For

When a bug is documented and understood, update the relevant diff model's `accounted_for` logic to mark these discrepancies:

```sql
-- Bug XXX: Description
-- See: docs/bugs/XXX-description.md
OR change_keys = ARRAY['field_name']::text []
```

## Current Bugs

- [Bug 001](./001-boe-lgc-pointer-two-digit-codes.md): BOE LGC Pointer incorrect for two-digit LGC codes (67 records, all Queens)

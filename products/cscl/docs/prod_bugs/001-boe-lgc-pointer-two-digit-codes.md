# Bug 001: BOE LGC Pointer Incorrect for Two-Digit LGC Codes

**Status:** Fixed in new ETL
**Affected Output:** LION .dat files (all boroughs)
**Severity:** Medium - Data accuracy issue
**Discrepancy Count:** 67 records (all in Queens)

## Summary

The legacy ETL incorrectly sets the Board of Elections LGC Pointer (`boe_lgc_pointer`) to "1" for all segments where the BOE preferred LGC is a two-digit code (e.g., "06", "11", "41"), regardless of which position that LGC actually appears in the segment's LGC list.

## Technical Details

### Expected Behavior

The `boe_lgc_pointer` field should contain an integer (1-9) indicating the **position** of the BOE preferred LGC in the segment's LGC list. For example:
- If the BOE preferred LGC appears at position 1 (LGC1), set `boe_lgc_pointer = "1"`
- If the BOE preferred LGC appears at position 2 (LGC2), set `boe_lgc_pointer = "2"`
- And so on...

### Actual Legacy Behavior

The legacy ETL code contains a defensive check that incorrectly treats two-digit LGC codes as invalid:

**Location:** `/cscl_etl_archive/ETL/CSCL.ETL.Extractor/Source Files/CenterlineExtractFile.cs:1318-1323`

```csharp
//Get the BOE LGC (returns the actual LGC code, e.g., "11")
string boeLGC = CSCLDataHelpersClass.GetBOEPreferredLGC(...);

int i;
if (System.Int32.TryParse(boeLGC, out i))
{
    //Check the value
    if (i > 9)  // ❌ BUG: Assumes LGC codes should be single-digit
    {
        return "1";  // Incorrectly defaults to "1"
    }
    else
    {
        // Search for LGC in positions 1-9 (this logic is correct but never executed for 2-digit codes)
        if (i == lgc) return "1";  // Check LGC1
        if (i == lgc) return "2";  // Check LGC2
        // ... etc
    }
}
```

**The Problem:**
- LGC codes are 2-character strings (can be "01" through "99+") per the schema specification
- The code parses the LGC code as an integer and checks `if (i > 9)`
- For two-digit codes like "11", this evaluates to TRUE
- Returns "1" immediately without searching for the actual position
- The correct logic in the `else` block is never executed

### Example: Segment 9012075

**Segment LGC Data:**
```
segmentid | lgc_rank | lgc | boe_preferred_lgc_flag
-----------+----------+-----+------------------------
  9012075 |        1 | 06  | N
  9012075 |        2 | 11  | Y  ← BOE Preferred
  9012075 |        3 | 41  | N
```

**Legacy Output:** `boe_lgc_pointer = "1"` ❌
**Correct Output:** `boe_lgc_pointer = "2"` ✓ (LGC "11" is at rank 2)

## Root Cause

The developer incorrectly assumed LGC codes should be single-digit (1-9), when in reality:
- LGC codes are defined as `Char(2)` in the schema
- Valid codes include "01", "06", "11", "41", etc.
- The system supports up to 9 LGC entries per segment (positions 1-9)
- The pointer should indicate **position**, not validate the **code value**

## New ETL Implementation

The new dbt-based ETL correctly calculates the BOE LGC pointer:

**Location:** `products/cscl/models/intermediate/street_and_face_code/int__streetcode_and_facecode.sql:19`

```sql
MAX(CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank END) AS boe_lgc_pointer
```

This directly returns the **rank position** of the LGC where `boe_preferred_lgc_flag = 'Y'`, which is the intended behavior.

## Impact Assessment

**Affected Records:** 67 segments (all in Queens/Borough 4)

**Change Pattern:** All affected records show `old: "1" → new: "2"`, meaning:
- Legacy system incorrectly set pointer to "1"
- New system correctly identifies BOE preferred LGC at position 2

**Recommendation:** These discrepancies represent **data quality improvements** and should be marked as `accounted_for = true` in the diff tracking.

## References

- ETL Documentation: See Section on "Board of Elections LGC Pointer"
- Schema Definition: LGC fields are `Text | 2 | Char(2)`
- BOE_LGC output field: `Text | 1 | Char(1)` (pointer value 1-9)

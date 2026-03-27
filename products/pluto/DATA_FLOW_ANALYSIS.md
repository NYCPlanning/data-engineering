# PLUTO Data Flow Analysis

## Big Picture: How PLUTO Gets Built

### Phase 1: Setup (Preparation Tables)
```
01a_dbt_staging.sh
├── Creates staging tables from raw inputs
└── Sets up: pluto_rpad_geo, pluto_allocated, pluto_input_cama, etc.
```

### Phase 2: Main Build (02_build.sh)

#### Step 1: Create Empty PLUTO Table (line 21)
```sql
-- create.sql creates empty table with ~100 columns
CREATE TABLE pluto (...);
```

#### Step 2: INSERT Initial Rows (line 22)
```sql
-- bbl.sql - THIS IS WHERE ROWS ARE INSERTED
INSERT INTO pluto (bbl, borocode, borough, block, lot)
SELECT DISTINCT primebbl, ... 
FROM pluto_rpad_geo;
```
**This creates one row per BBL** (the grain of PLUTO)

#### Step 3: First Major Population (line 25)
```sql
-- allocated.sql - Adds ~25 fields from pluto_allocated
UPDATE pluto SET bldgclass, ownername, lotarea, bldgarea, ... 
FROM pluto_allocated WHERE pluto.bbl = pluto_allocated.bbl;
```

#### Step 4: Add Geocodes (line 28)
```sql
-- geocodes.sql - Adds ~20 fields from pluto_rpad_geo
UPDATE pluto SET cd, ct2010, schooldist, zipcode, address, ...
FROM pluto_rpad_geo WHERE pluto.bbl = pluto_rpad_geo.primebbl;
```

#### Step 5: Progressive Enhancement (lines 31-90)
Then 60+ SQL files run, each doing UPDATE statements to add/refine fields:
- CAMA fields (bsmttype, lottype, proxcode, easements)
- Zoning fields (splitzone, specialdistrict)
- Geocoding (dtmgeoms, latlong)
- Classifications (bldgclass)
- etc.

#### Step 6: DBT Integration (line 87-90)
```bash
dbt run --select tag:pluto_enrichment pluto_enriched
run_sql_file sql/apply_dbt_enrichments.sql
```
Creates pluto_enriched table (from dbt models), then copies ~28 fields back to pluto

### Phase 3: Corrections (03_corrections.sh)
Research-driven corrections to specific BBLs

---

## Answer to Your Questions

### 1️⃣ When are rows inserted?

**Line 22 of 02_build.sh: `bbl.sql`**

This is the ONLY place where rows are inserted into pluto:
```sql
INSERT INTO pluto (bbl, borocode, borough, block, lot)
SELECT DISTINCT primebbl, ... FROM pluto_rpad_geo;
```

Everything else is UPDATE statements that enrich these rows.

### 2️⃣ DBT Strategy - Two Approaches:

---

## Strategy A: "Big Bang" - Replace PLUTO with Pure DBT

### Concept
Stop doing UPDATE statements entirely. Build pluto as a single dbt model (or series of CTEs).

### Approach
```sql
-- models/product/pluto.sql
WITH base_bbls AS (
    SELECT DISTINCT primebbl as bbl, ... FROM pluto_rpad_geo
),

allocated_data AS (
    SELECT bbl, bldgclass, ownername, ... FROM pluto_allocated
),

geocode_data AS (
    SELECT primebbl as bbl, cd, zipcode, ... FROM pluto_rpad_geo
),

-- Import existing dbt models
far_data AS (SELECT * FROM {{ ref('far') }}),
landuse_data AS (SELECT * FROM {{ ref('landuse') }}),
... all other dbt models ...

-- New dbt models for remaining SQL
cama_data AS (SELECT * FROM {{ ref('cama_bsmttype') }}),
zoning_data AS (SELECT * FROM {{ ref('zoning_splitzone') }}),
...

final AS (
    SELECT 
        base_bbls.*,
        allocated_data.* EXCLUDE (bbl),
        geocode_data.* EXCLUDE (bbl),
        far_data.* EXCLUDE (bbl),
        cama_data.* EXCLUDE (bbl),
        zoning_data.* EXCLUDE (bbl),
        ...
    FROM base_bbls
    LEFT JOIN allocated_data USING (bbl)
    LEFT JOIN geocode_data USING (bbl)
    LEFT JOIN far_data USING (bbl)
    LEFT JOIN cama_data USING (bbl)
    LEFT JOIN zoning_data USING (bbl)
    ...
)

SELECT * FROM final
```

### Pros
- ✅ Pure declarative dbt - no imperative UPDATEs
- ✅ Easy to test, debug, and reason about
- ✅ Can regenerate entire PLUTO from scratch
- ✅ Clear dependency graph in dbt

### Cons
- ❌ Big migration effort - all at once
- ❌ Need to rewrite complex UPDATE logic (e.g., spatial joins, geocoding)
- ❌ May be harder to preserve exact current behavior

---

## Strategy B: "Progressive Migration" (RECOMMENDED)

### Concept
Keep the current UPDATE pattern but gradually move logic to dbt models.

### Current State (What You've Done)
```bash
# Phase 1: SQL creates and populates base pluto
run_sql_file sql/create.sql
run_sql_file sql/bbl.sql        # INSERT rows
run_sql_file sql/allocated.sql  # UPDATE with allocated data
run_sql_file sql/geocodes.sql   # UPDATE with geocode data
... more SQL UPDATEs ...

# Phase 2: DBT enrichment
dbt run --select tag:pluto_enrichment pluto_enriched

# Phase 3: Apply dbt results
run_sql_file sql/apply_dbt_enrichments.sql  # Copies from pluto_enriched
```

### Next Steps for Progressive Migration

#### Wave 1: CAMA + Zoning + Classification (SAFE - No dependencies)
Convert these to dbt models:
- `cama_bsmttype.sql` → `models/intermediate/cama/cama_bsmttype.sql`
- `cama_lottype.sql` → `models/intermediate/cama/cama_lottype.sql`
- `cama_proxcode.sql` → `models/intermediate/cama/cama_proxcode.sql`
- `cama_easements.sql` → `models/intermediate/cama/cama_easements.sql`
- `zoning_splitzone.sql` → `models/intermediate/zoning/zoning_splitzone.sql`
- `zoning_specialdistrict.sql` → `models/intermediate/zoning/zoning_specialdistrict.sql`
- `bldgclass.sql` → `models/intermediate/simple/bldgclass.sql`

Then:
1. Add these models to pluto_enriched (add LEFT JOINs)
2. Add fields to apply_dbt_enrichments.sql SET clause
3. Remove run_sql_file calls from 02_build.sh
4. Delete original SQL files

#### Wave 2: Prime BBL + Year Built (Separate tables)
- `primebbl.sql` → Updates dof_pts_propmaster, pluto_rpad_geo
- `yearbuiltalt.sql` → Updates pluto_allocated

These are trickier because they update tables OTHER than pluto.
Options:
  a) Leave as SQL (they're prep tables)
  b) Move to dbt staging models that materialize these tables

#### Wave 3: Geocoding (Test DBT dependencies)
- `latlong.sql` → Currently runs BEFORE dbt
- `dtmgeoms.sql` → Geometry operations

Need to verify: Do any existing dbt models read centroid/lat/long from pluto?
If yes, either:
  a) Keep these as SQL before dbt run
  b) Create dbt source for these fields and manage dependencies

#### Wave 4: Remaining
- `plutomapid.sql`, `versions.sql`, `numericfields_geomfields.sql`

---

## Recommended DBT Strategy: Progressive Migration

### Why This Works Better

1. **Incremental risk** - Migrate 7 files in Wave 1, test, then proceed
2. **Preserves current pattern** - Keep the pluto table as mutable state
3. **Easy rollback** - Just revert apply_dbt_enrichments.sql changes
4. **Clear testing** - After each wave, dbt run + 02_build.sh should work

### The DBT Architecture Should Be:

```
models/
├── intermediate/
│   ├── cama/
│   │   ├── cama_bsmttype.sql     (NEW - Wave 1)
│   │   ├── cama_lottype.sql      (NEW - Wave 1)
│   │   ├── cama_proxcode.sql     (NEW - Wave 1)
│   │   └── cama_easements.sql    (NEW - Wave 1)
│   ├── zoning/
│   │   ├── zoning_splitzone.sql  (NEW - Wave 1)
│   │   └── zoning_specialdistrict.sql (NEW - Wave 1)
│   └── simple/
│       ├── far.sql               (DONE)
│       ├── landuse.sql           (DONE)
│       ├── bldgclass.sql         (NEW - Wave 1)
│       └── ...
└── product/
    └── pluto_enriched.sql        (Expand with Wave 1 models)
```

### Modified 02_build.sh After Wave 1:
```bash
# Lines 33-40: Remove these run_sql_file calls
# run_sql_file sql/cama_bsmttype.sql    ❌ DELETE
# run_sql_file sql/cama_lottype.sql     ❌ DELETE  
# run_sql_file sql/cama_proxcode.sql    ❌ DELETE
# run_sql_file sql/cama_easements.sql   ❌ DELETE

# Line 67: Remove this
# run_sql_file sql/zoning_splitzone.sql ❌ DELETE

# Line 63: Remove this  
# run_sql_file sql/zoning_specialdistrict.sql ❌ DELETE

# Line 71: Remove this
# run_sql_file sql/bldgclass.sql        ❌ DELETE

# Line 87: DBT run now includes Wave 1 models
(cd .. && dbt run --select tag:pluto_enrichment pluto_enriched)

# Line 90: apply_dbt_enrichments.sql now includes Wave 1 fields
run_sql_file sql/apply_dbt_enrichments.sql
```

---

## Key Insight: The Current Pattern Is Actually Good!

The current hybrid approach (SQL UPDATEs + dbt enrichment + apply back) is working well because:

1. **Keeps complex spatial/imperative logic in SQL** where it's easier
2. **Moves declarative transformations to dbt** where they belong
3. **pluto_enriched acts as staging** - dbt builds it clean, SQL applies it
4. **Incremental migration path** - doesn't require rewriting everything

The goal isn't to eliminate the pluto table and UPDATEs entirely - it's to move the **business logic** to dbt while keeping the **table building mechanics** in SQL.


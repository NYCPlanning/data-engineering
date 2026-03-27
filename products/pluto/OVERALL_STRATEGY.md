# PLUTO DBT Migration: Overall Strategy

## The Core Problem: A Mutable PLUTO Table

PLUTO is fundamentally a **mutable state table** that gets progressively enriched through ~60+ UPDATE operations. This is the reality we need to design around.

---

## Current Architecture: Three-Phase Build

### Phase 1: Initial Population (SQL)
```sql
-- 1. Create empty table
CREATE TABLE pluto (...);

-- 2. INSERT rows (one per BBL)
INSERT INTO pluto (bbl, borocode, borough, block, lot)
SELECT DISTINCT primebbl, ... FROM pluto_rpad_geo;

-- 3. First major enrichment from allocated table
UPDATE pluto SET bldgclass, ownername, lotarea, bldgarea, ...
FROM pluto_allocated WHERE pluto.bbl = pluto_allocated.bbl;

-- 4. Second major enrichment from geocodes
UPDATE pluto SET cd, ct2010, schooldist, zipcode, address, ...
FROM pluto_rpad_geo WHERE pluto.bbl = pluto_rpad_geo.primebbl;
```
**Result:** ~850k BBL rows with ~45 fields populated

### Phase 2: Progressive Enhancement (SQL → DBT → SQL)
```sql
-- 60+ SQL files doing targeted UPDATEs
run_sql_file sql/cama_bsmttype.sql      # Sets bsmtcode
run_sql_file sql/cama_lottype.sql       # Sets lottype
run_sql_file sql/zoning_splitzone.sql   # Sets splitzone (multiple UPDATEs in file)
run_sql_file sql/latlong.sql            # Sets latitude, longitude, centroid
... 50+ more files ...

-- DBT enrichment step
dbt run --select tag:pluto_enrichment pluto_enriched

-- Apply DBT results back
run_sql_file sql/apply_dbt_enrichments.sql  # Copies 28 fields from pluto_enriched
```
**Result:** Fully enriched PLUTO with ~100 fields

### Phase 3: Corrections (SQL)
```sql
-- Research-driven corrections (runs in separate script)
run_sql_file sql/corr_lotarea.sql  # Fixes specific BBLs, recalculates builtfar
```

---

## Key Finding: Fields Updated Multiple Times

### Analysis Results:

**7 fields get updated multiple times:**

1. **builtfar** (3 updates)
   - `apply_dbt_enrichments.sql` → Initial calculation from dbt
   - `backfill.sql` → Backfills missing values (NOT USED - orphaned file)
   - `corr_lotarea.sql` → Research corrections override

2. **centroid** (2 updates)
   - `latlong.sql` → Initial calculation (BEFORE dbt)
   - `apply_dbt_enrichments.sql` → Overwrites from dbt models

3. **sanitdistrict** (2 updates)
   - `numericfields_geomfields.sql` → Initial set (BEFORE dbt)
   - `apply_dbt_enrichments.sql` → Overwrites from dbt models

4. **splitzone** (6 updates in 2 files)
   - `zoning_splitzone.sql` → Multiple progressive UPDATEs
   - `zoning.sql` → Additional refinement UPDATEs

5. **spdist1, spdist2** (7 updates each)
   - `zoning_specialdistrict.sql` → Multiple progressive UPDATEs

6. **plutomapid** (appears 2x, same file)
   - `plutomapid.sql` → Multiple UPDATEs in same file

**Pattern:** Most "multi-updates" are either:
- Multiple UPDATEs within the SAME file (progressive refinement)
- Corrections phase overriding main build
- Pre-dbt SQL setting values that dbt later overwrites

---

## The DBT Strategy: Hybrid Architecture

### ❌ Why NOT Full DBT Replacement?

**Don't try to build pluto as a single dbt model because:**

1. **Spatial operations are hard in dbt**
   - Geometry calculations (ST_AREA, ST_INTERSECTION, ST_COVEREDBY)
   - Complex spatial joins with ranking/ordering
   - Much easier in procedural SQL

2. **Multi-step refinement pattern**
   - Files like `zoning_specialdistrict.sql` do 7+ UPDATEs with temp tables
   - This is imperative logic, not declarative
   - Would be ugly as nested CTEs

3. **Corrections need to override**
   - Research-driven corrections in Phase 3 need to overwrite computed values
   - Easier to let corrections UPDATE the final table

4. **BBL creation is inherently procedural**
   - INSERT from pluto_rpad_geo
   - JOIN allocated data
   - JOIN geocode data
   - This isn't broken, don't fix it

### ✅ What We Should DBT'ify

**Move declarative business logic to dbt, keep table-building in SQL.**

#### Category 1: Pure Transformations (SAFE - Already doing this)
Files that calculate values from source data with no complex logic:
- ✅ `far.sql` → Already in dbt
- ✅ `landuse.sql` → Already in dbt
- ✅ `irrlotcode.sql` → Already in dbt
- ✅ `sanitboro.sql` → Already in dbt
- etc. (13 files already migrated)

#### Category 2: Simple Lookups/Defaults (WAVE 1 - Next to migrate)
Files that do straightforward lookups or set defaults:
- `cama_bsmttype.sql` → SET bsmtcode = '5' WHERE NULL
- `cama_lottype.sql` → SET lottype = '0' WHERE NULL
- `cama_proxcode.sql` → SET proxcode = '0' WHERE NULL
- `cama_easements.sql` → SET easements = '0' WHERE NULL
- `bldgclass.sql` → SET bldgclass = 'Q0' WHERE zonedist1 = 'PARK'

**These are perfect dbt candidates:** Simple CASE/COALESCE logic

#### Category 3: Complex Spatial/Multi-Step (KEEP AS SQL)
Files that do complex spatial operations or progressive refinement:
- `zoning_specialdistrict.sql` → 7 UPDATEs with spatial ranking
- `zoning_splitzone.sql` → Multiple progressive updates
- `dtmgeoms.sql` → Geometry operations
- `spatialjoins.sql` → Political boundaries via spatial join

**Keep these as SQL:** They're hard to express declaratively

#### Category 4: Pre-DBT Dependencies (MIGRATE CAREFULLY)
Files that run BEFORE dbt and might be read by dbt models:
- `latlong.sql` → Sets centroid (dbt models may reference)
- `numericfields_geomfields.sql` → Sets sanitdistrict (dbt models reference)

**Strategy:** Either keep as pre-dbt SQL OR migrate and update dbt dependencies

---

## The Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: SQL Creates Initial PLUTO                          │
│ - INSERT rows from pluto_rpad_geo                           │
│ - UPDATE from allocated (25 fields)                         │
│ - UPDATE from geocodes (20 fields)                          │
│ - UPDATE from spatial/complex SQL (geometry, zoning)        │
│ Result: PLUTO with ~60 fields populated                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: DBT Enrichment                                     │
│ - Read from pluto table (current state)                     │
│ - Read from source tables (allocated, geocodes, etc.)       │
│ - Calculate business logic fields                           │
│ - Output: pluto_enriched (bbl + calculated fields)          │
│                                                              │
│ DBT Models:                                                  │
│ - far (existing)                                             │
│ - landuse (existing)                                         │
│ - cama_bsmttype (NEW - Wave 1)                              │
│ - cama_lottype (NEW - Wave 1)                               │
│ - bldgclass (NEW - Wave 1)                                  │
│ - ... etc                                                    │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 3: SQL Applies DBT Results                            │
│ - UPDATE pluto SET (fields) = pluto_enriched.(fields)       │
│ - Result: Fully enriched PLUTO                              │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 4: Corrections (Separate Script)                      │
│ - Research-driven overrides for specific BBLs               │
│ - Result: Corrected PLUTO ready for export                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Migration Waves (Progressive Strategy)

### Wave 1: Simple Defaults/Lookups (7 files)
**What:** CAMA defaults, classification rules, simple zoning
**Why safe:** Pure calculations, no spatial logic, no dependencies
**Files:**
- cama_bsmttype.sql → `SET bsmtcode = '5' WHERE NULL`
- cama_lottype.sql → `SET lottype = '0' WHERE NULL`
- cama_proxcode.sql → `SET proxcode = '0' WHERE NULL`
- cama_easements.sql → `SET easements = '0' WHERE NULL`
- bldgclass.sql → `SET bldgclass = 'Q0' WHERE zonedist1 = 'PARK'`
- zoning_splitzone.sql → Splitzone logic (may have multiple updates)
- zoning_specialdistrict.sql → Special district logic (may have multiple updates)

**Migration approach:**
1. Create dbt model that outputs `bbl + field`
2. Add LEFT JOIN to pluto_enriched.sql
3. Add field to apply_dbt_enrichments.sql SET clause
4. Remove run_sql_file from 02_build.sh
5. Delete original SQL file

### Wave 2: Separate Tables (2 files)
**What:** Updates to non-pluto tables
**Files:**
- primebbl.sql → Updates dof_pts_propmaster, pluto_rpad_geo
- yearbuiltalt.sql → Updates pluto_allocated

**Strategy:** Consider these staging/prep - maybe leave as SQL or move to dbt staging models

### Wave 3: Pre-DBT Dependencies (3 files)
**What:** Files that run before dbt and may be read by dbt models
**Files:**
- latlong.sql → centroid (may be referenced by dbt)
- numericfields_geomfields.sql → sanitdistrict (IS referenced by dbt)
- dtmgeoms.sql → geometry operations

**Strategy:** 
- Audit existing dbt models to see what they reference
- Either keep as pre-dbt SQL OR migrate carefully with dependency updates

### Wave 4: Post-DBT & Misc (3+ files)
**What:** Files that run after dbt or are miscellaneous
**Files:**
- plutomapid.sql → Runs after dbt integration
- versions.sql → Version stamping
- apdate.sql → Date formatting for pluto_rpad_geo

---

## Handling Multi-Update Fields

### Pattern 1: Progressive Refinement in Same File
**Example:** `zoning_specialdistrict.sql` does 7 UPDATEs

**Strategy:** Keep as SQL - this is imperative logic that's hard to express in dbt

### Pattern 2: Pre-DBT SQL → DBT Overwrites
**Example:** 
- `latlong.sql` sets centroid
- Then dbt models might recalculate it
- `apply_dbt_enrichments.sql` overwrites

**Strategy:** Choose the authoritative source:
- If dbt version is better → migrate latlong logic to dbt, remove SQL file
- If SQL version is needed first → keep SQL, don't overwrite in dbt

### Pattern 3: Main Build → Corrections Override
**Example:**
- `apply_dbt_enrichments.sql` sets builtfar
- `corr_lotarea.sql` (Phase 3) corrects specific BBLs

**Strategy:** This is fine! Corrections should override. Keep both.

---

## Key Principles

1. **PLUTO is mutable state - embrace it**
   - Don't try to make it a pure dbt model
   - INSERT + UPDATE pattern works fine

2. **DBT for declarative logic**
   - Move CASE statements, lookups, calculations to dbt
   - Keep spatial operations, progressive refinement in SQL

3. **Progressive migration**
   - Migrate simple files first (Wave 1)
   - Leave complex spatial logic in SQL

4. **pluto_enriched is the bridge**
   - DBT outputs bbl + calculated fields
   - SQL applies them back to pluto
   - This hybrid approach is actually elegant

5. **Corrections override everything**
   - Research-driven corrections in Phase 3 are final
   - They should UPDATE the computed values

---

## Should We DBT'ify pluto_rpad_geo?

**Question:** Should the initial pluto creation (INSERT from pluto_rpad_geo, UPDATE from allocated/geocodes) be moved to dbt?

**Answer:** **Maybe, but low priority.**

**Pros:**
- ✅ Could create pluto as a dbt model via LEFT JOINs
- ✅ More declarative, easier to understand

**Cons:**
- ❌ Current SQL pattern works fine
- ❌ Would need to handle ~850k rows in dbt materialization
- ❌ Corrections phase still needs mutable table
- ❌ Lots of migration effort for marginal benefit

**Recommendation:** 
Focus on migrating the 60+ UPDATE files to dbt first. If that works well and we want to go further, THEN consider dbt'ifying the initial population. But the current pattern of:
```
SQL creates/populates → DBT enriches → SQL applies → SQL corrects
```
is actually quite clean.

---

## Success Metrics

After full migration, we should have:

1. **~40 SQL files removed** (simple business logic moved to dbt)
2. **~20-30 SQL files remaining** (spatial operations, initial population, corrections)
3. **~40 dbt models** in intermediate/ (one per migrated SQL file)
4. **1 dbt model** in product/ (pluto_enriched, expanded with new models)
5. **Clear separation**: SQL for data engineering, DBT for business logic


# Should pluto_rpad_geo Be a DBT Model?

## Current State

### What is pluto_rpad_geo?
A **critical intermediate table** that joins DOF property tax data with DCP geocodes.

### How It's Created (line 8 of 02_build.sh)
```sql
-- sql/create_pts.sql
-- Transforms pluto_pts (raw PTS data) → dof_pts_propmaster
CREATE TABLE dof_pts_propmaster AS 
SELECT boro, block, lot, owner, land_area, gross_sqft, ...
FROM pluto_pts;  -- Already a dbt staging model (stg__pluto_pts)

-- sql/create_rpad_geo.sql
-- Joins property tax data with geocodes
CREATE TABLE pluto_rpad_geo AS
WITH pluto_rpad_rownum AS (
    SELECT 
        dof_pts_propmaster.*,
        ROW_NUMBER() OVER (
            PARTITION BY boro || tb || tl
            ORDER BY curavt_act DESC, land_area DESC
        ) AS row_number
    FROM dof_pts_propmaster
)
SELECT 
    pluto_rpad_sub.*,
    stg__pluto_input_geocodes.*
FROM pluto_rpad_rownum pluto_rpad_sub
LEFT JOIN stg__pluto_input_geocodes 
    ON [bbl join condition]
WHERE row_number = 1;

-- Then 5 more SQL files modify it:
UPDATE pluto_rpad_geo SET bbl = ...
UPDATE pluto_rpad_geo SET xcoord = ..., ycoord = ...
-- (zerovacantlots.sql, lotarea.sql, primebbl.sql, apdate.sql)
```

### What Uses pluto_rpad_geo? (9 files)
1. **bbl.sql** - INSERTs initial rows into pluto
2. **geocodes.sql** - Major UPDATE to pluto (~20 fields)
3. **create_allocated.sql** - Creates pluto_allocated table
4. **address.sql** - Address processing
5. **cama_bldgarea_1.sql** - Building area calculations
6. **lotarea.sql** - Lot area processing
7. **bldgclass.sql** - Building classification
8. **geocode_notgeocoded.sql** - Geocoding fixes
9. **create_rpad_geo.sql** - Self-reference for UPDATEs

---

## The DBT Question

### Option A: Keep pluto_rpad_geo as SQL (Current State)
```
pluto_pts (raw) 
  → [dbt staging] stg__pluto_pts
  → [SQL] dof_pts_propmaster (create_pts.sql)
  → [SQL] pluto_rpad_geo (create_rpad_geo.sql)
  → [SQL] 5 UPDATE statements modify it
  → [SQL] Used by bbl.sql, geocodes.sql, etc.
```

### Option B: Make pluto_rpad_geo a DBT Model ✅ RECOMMENDED
```
pluto_pts (raw)
  → [dbt staging] stg__pluto_pts
  → [dbt intermediate] int__dof_pts_propmaster (replaces create_pts.sql)
  → [dbt intermediate] int__pluto_rpad_geo (replaces create_rpad_geo.sql)
  → [SQL] Used by bbl.sql, geocodes.sql, etc. (read-only)
```

---

## Analysis: Should We DBT'ify pluto_rpad_geo?

### ✅ YES - Strong Arguments For

#### 1. **Targeted Re-running** (Your Key Requirement)
Currently, to rebuild pluto_rpad_geo, you must:
- Run 01a_dbt_staging.sh (all staging models)
- Run lines 6-14 of 02_build.sh (preprocessing → apdate.sql)

With dbt:
```bash
# Just rebuild the specific model and downstream
dbt run --select int__pluto_rpad_geo+
```

#### 2. **It's Already Mostly DBT-Ready**
The CREATE statement is a clean SELECT with CTEs:
- Window function for deduplication ✓
- LEFT JOIN between two tables ✓
- No complex procedural logic ✓

The transformation logic is **declarative**, perfect for dbt.

#### 3. **Eliminates Mutable State Issues**
Current problem: pluto_rpad_geo gets modified by 5 SQL files after creation.
With dbt: All logic in one model, no mutation needed.

#### 4. **Better Dependency Management**
dbt DAG would show:
```
stg__pluto_pts ──┐
                 ├─> int__dof_pts_propmaster ──┐
stg__pluto_input_geocodes ─────────────────────┤
                                                ├─> int__pluto_rpad_geo
                                                     (used by SQL scripts)
```

#### 5. **Easier to Test & Debug**
- dbt tests on data quality
- Can run in isolation
- Version control changes clearly
- Can use dbt snapshots for auditing

#### 6. **The UPDATEs Can Be Incorporated**
Those 5 UPDATE statements that modify pluto_rpad_geo?
They're just transformations that can be CASE/COALESCE in the dbt model:

```sql
-- Instead of: UPDATE pluto_rpad_geo SET bbl = ...
-- Do: SELECT CASE WHEN ... THEN ... END AS bbl

-- Instead of: UPDATE pluto_rpad_geo SET xcoord = ...
-- Do: SELECT COALESCE(xcoord, ST_X(...)) AS xcoord
```

### ⚠️ Considerations

#### 1. **Migration Effort**
Need to:
- Convert create_pts.sql → int__dof_pts_propmaster.sql (medium effort)
- Convert create_rpad_geo.sql → int__pluto_rpad_geo.sql (medium effort)
- Incorporate 5 UPDATE statements into the model (low effort)
- Update 9 SQL files to reference dbt model instead of table (low effort)

**Estimate: 2-3 hours of work**

#### 2. **SQL Files Need to Read from DBT Output**
The 9 SQL files that reference pluto_rpad_geo will now read from a dbt model.

**Solution:** Materialize as table in dbt, SQL continues to work:
```sql
-- models/intermediate/int__pluto_rpad_geo.sql
{{ config(materialized='table') }}

-- ... model logic ...
```

SQL files reference it the same way:
```sql
SELECT * FROM int__pluto_rpad_geo;  -- Works just like pluto_rpad_geo
```

#### 3. **Build Script Changes**
```bash
# OLD: 02_build.sh lines 6-14
run_sql_file sql/preprocessing.sql
run_sql_file sql/create_pts.sql
run_sql_file sql/create_rpad_geo.sql
run_sql_file sql/zerovacantlots.sql  # UPDATEs pluto_rpad_geo
run_sql_file sql/lotarea.sql         # UPDATEs pluto_rpad_geo
run_sql_file sql/primebbl.sql        # UPDATEs pluto_rpad_geo
run_sql_file sql/apdate.sql          # UPDATEs pluto_rpad_geo

# NEW: 02_build.sh
run_sql_file sql/preprocessing.sql
(cd .. && dbt run --select int__dof_pts_propmaster int__pluto_rpad_geo)
# No more create_pts, create_rpad_geo, zerovacantlots, lotarea, primebbl, apdate
```

**Those 5 UPDATE files get deleted** - their logic absorbed into the dbt model.

---

## Recommendation: **YES, DBT'ify pluto_rpad_geo**

### Why It's Worth It

1. **Solves your targeted re-running problem** - `dbt run --select int__pluto_rpad_geo`
2. **Minimal risk** - It's used read-only by downstream SQL
3. **Removes mutable state** - No more UPDATE statements on rpad_geo
4. **Clean separation** - DBT handles data prep, SQL handles pluto building
5. **Not a massive refactor** - 2-3 hours, well-scoped work

### The New Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ DBT Phase 1: Staging (01a_dbt_staging.sh)                   │
│ - stg__pluto_pts (from raw pluto_pts)                       │
│ - stg__pluto_input_geocodes (from raw geocodes)             │
│ - stg__pluto_input_cama_dof (from raw CAMA)                 │
│ - ... all other staging models ...                          │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ DBT Phase 2: Intermediate Prep Tables (NEW - add to build)  │
│ - int__dof_pts_propmaster (from stg__pluto_pts)             │
│ - int__pluto_rpad_geo (from int__dof_pts + stg__geocodes)   │
│   ↑ Includes all the UPDATE logic (bbl calc, coords, etc.)  │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ SQL Phase 1: Build Base PLUTO (02_build.sh)                 │
│ - CREATE TABLE pluto                                         │
│ - INSERT from int__pluto_rpad_geo (read-only)               │
│ - UPDATE from pluto_allocated                                │
│ - UPDATE from geocodes (reads int__pluto_rpad_geo)          │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ SQL Phase 2: Progressive Enhancement                         │
│ - CAMA updates, zoning, geocoding, etc.                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ DBT Phase 3: Business Logic Enrichment                      │
│ - far, landuse, irrlotcode, etc.                            │
│ - Output: pluto_enriched                                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ SQL Phase 3: Apply DBT Results + Corrections                │
│ - UPDATE pluto from pluto_enriched                           │
│ - Research corrections                                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Migration Plan

### Step 1: Create int__dof_pts_propmaster.sql
```sql
-- models/intermediate/int__dof_pts_propmaster.sql
{{ config(
    materialized='table',
    indexes=[{'columns': ['boro', 'tb', 'tl'], 'unique': False}]
) }}

SELECT
    boro,
    block AS tb,
    lot AS tl,
    parid AS bbl,
    -- ... all the field transformations from create_pts.sql ...
FROM {{ ref('stg__pluto_pts') }}
```

### Step 2: Create int__pluto_rpad_geo.sql
```sql
-- models/intermediate/int__pluto_rpad_geo.sql
{{ config(
    materialized='table',
    indexes=[{'columns': ['primebbl'], 'unique': False}]
) }}

WITH 
-- Deduplicate property tax records
pluto_rpad_dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY boro || tb || tl
            ORDER BY curavt_act DESC, land_area DESC, ease ASC
        ) AS row_number
    FROM {{ ref('int__dof_pts_propmaster') }}
),

-- Join with geocodes
base_join AS (
    SELECT 
        pts.*,
        geo.* EXCLUDE (geo_bbl)
    FROM pluto_rpad_dedup pts
    LEFT JOIN {{ ref('stg__pluto_input_geocodes') }} geo
        ON pts.boro || pts.tb || pts.tl = 
           geo.borough || LPAD(geo.block, 5, '0') || LPAD(geo.lot, 4, '0')
    WHERE pts.row_number = 1
),

-- Incorporate zerovacantlots.sql logic
with_vacant_fixes AS (
    SELECT *,
        CASE 
            WHEN land_area = 0 AND [vacant lot conditions]
            THEN [calculate from geometry]
            ELSE land_area
        END AS land_area_fixed
    FROM base_join
),

-- Incorporate lotarea.sql logic
-- Incorporate primebbl.sql logic
-- Incorporate apdate.sql logic
-- Incorporate geocode_billingbbl.sql logic

final AS (
    SELECT
        -- All fields with transformations applied
        boro || LPAD(tb, 5, '0') || LPAD(tl, 4, '0') AS bbl,
        boro || LPAD(tb, 5, '0') || LPAD(tl, 4, '0') AS primebbl,
        to_char(to_date(ap_date, 'MM/DD/YY'), 'MM/DD/YYYY') AS ap_datef,
        COALESCE(xcoord, ST_X(ST_TRANSFORM(geom, 2263))::integer) AS xcoord,
        COALESCE(ycoord, ST_Y(ST_TRANSFORM(geom, 2263))::integer) AS ycoord,
        -- ... all other fields ...
    FROM with_vacant_fixes
)

SELECT * FROM final
```

### Step 3: Update 02_build.sh
```bash
# Before line 21 (create.sql), add:
echo 'Building intermediate prep tables'
(cd .. && dbt run --select int__dof_pts_propmaster int__pluto_rpad_geo)

# Remove these lines:
# run_sql_file sql/create_pts.sql
# run_sql_file sql/create_rpad_geo.sql  
# run_sql_file sql/zerovacantlots.sql
# run_sql_file sql/lotarea.sql
# run_sql_file sql/primebbl.sql
# run_sql_file sql/apdate.sql
# run_sql_file sql/geocode_billingbbl.sql
```

### Step 4: Update SQL Files (9 files)
Change references from `pluto_rpad_geo` to `int__pluto_rpad_geo`:
- bbl.sql
- geocodes.sql
- create_allocated.sql
- etc.

### Step 5: Delete Obsolete SQL Files (7 files)
- create_pts.sql ❌
- create_rpad_geo.sql ❌
- zerovacantlots.sql ❌
- lotarea.sql ❌
- primebbl.sql ❌
- apdate.sql ❌
- geocode_billingbbl.sql ❌

---

## Benefits Summary

✅ **Targeted re-running** - Your core requirement
✅ **Removes 7 SQL files** - Simpler codebase
✅ **Eliminates mutable intermediate state** - pluto_rpad_geo becomes read-only
✅ **Better dependency visibility** - dbt DAG shows relationships
✅ **Easier debugging** - Single model vs scattered UPDATEs
✅ **Testable** - dbt tests on data quality
✅ **Version controlled transformations** - All logic in one place

**Effort: 2-3 hours | Risk: Low | Value: High**

---

## Next Steps

1. Create issue: "DBT'ify pluto_rpad_geo intermediate table"
2. Implement int__dof_pts_propmaster.sql
3. Implement int__pluto_rpad_geo.sql (with absorbed UPDATE logic)
4. Test: dbt run --select int__pluto_rpad_geo
5. Update 02_build.sh and 9 SQL files
6. Delete 7 obsolete SQL files
7. Run full build to verify


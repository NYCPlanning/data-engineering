# PLUTO DBT Migration - Analysis Session Summary

**Date:** 2026-03-30
**Session Goal:** Analyze PLUTO SQL dependencies and create comprehensive migration strategy

---

## 🎯 What Was Accomplished

### 1. Dependency Analysis (Issue de-74o.14 - Closed)
**Created:** `products/pluto/dependencies.txt` (25KB)

Analyzed all SQL files in pluto_build for UPDATE statements:
- **42 UPDATE statements** found across **23 files**
- **29 updates** target the main PLUTO table
- **7 fields** updated multiple times (builtfar, centroid, sanitdistrict, splitzone, spdist1, spdist2, plutomapid)
- **No circular dependencies** detected
- **Critical finding:** backfill.sql is unused/orphaned, corr_lotarea.sql runs in separate phase

**Key insights:**
- Most "multi-updates" are progressive refinement within same file (zoning doing 6-7 UPDATEs)
- Some are pre-dbt SQL → dbt overwrites (centroid, sanitdistrict)
- One is corrections overriding computed values (builtfar) - expected behavior

### 2. Data Flow Analysis
**Created:** `products/pluto/DATA_FLOW_ANALYSIS.md` (8.6KB)

Traced how PLUTO is built:
- **Row insertion:** `bbl.sql` (line 22) - this is the ONLY place rows enter PLUTO
- **Initial population:** allocated.sql (~25 fields), geocodes.sql (~20 fields)
- **Progressive enhancement:** 60+ SQL files do targeted UPDATEs
- **DBT integration:** dbt run → pluto_enriched → apply_dbt_enrichments.sql
- **Corrections:** Separate script (03_corrections.sh) overrides computed values

**Answer to key question:**
- Keep initial PLUTO population as SQL (INSERT + first UPDATEs work fine)
- Focus on migrating the 60+ UPDATE files to dbt
- Hybrid SQL/DBT architecture is actually optimal

### 3. Overall Strategy
**Created:** `products/pluto/OVERALL_STRATEGY.md` (13.5KB)

Comprehensive strategy document explaining:
- PLUTO as mutable state table (embrace it, don't fight it)
- Why hybrid SQL/DBT is better than pure dbt (for now)
- What to migrate vs what to keep as SQL
- Multi-update field analysis and handling strategies
- 4 migration waves with clear boundaries

**Key principle:** 
- SQL for data engineering (INSERT, spatial ops, corrections)
- DBT for business logic (calculations, lookups, transformations)
- pluto_enriched as the bridge between them

### 4. pluto_rpad_geo Analysis
**Created:** `products/pluto/RPAD_GEO_DBT_ANALYSIS.md` (13.9KB)

**Strong YES recommendation** to dbt'ify pluto_rpad_geo as Wave 0:

**Why:**
- Solves targeted re-running requirement (main ask)
- Removes 7 SQL files by absorbing their logic
- Eliminates mutable intermediate state
- Better dependency management
- 2-3 hour effort, low risk, high value

**What it creates:**
- `int__dof_pts_propmaster.sql` - Property tax transformations
- `int__pluto_rpad_geo.sql` - Joins property tax + geocodes + all UPDATE logic

**Files that get deleted:**
- create_pts.sql, create_rpad_geo.sql, zerovacantlots.sql, lotarea.sql, primebbl.sql, apdate.sql, geocode_billingbbl.sql

### 5. Epic Updated
**Epic de-74o** now includes:
- Links to all 4 documentation files (required reading)
- Migration wave breakdown (0-4)
- Conversion pattern and requirements
- Key findings summary (dependencies, multi-updates, ordering)
- Quality gates (testing, linting)
- **End goal:** Pure dbt project (all SQL scripts eliminated)

---

## 📊 Key Findings Summary

### Execution Order
```
1. pluto_rpad_geo created (Wave 0 target)
2. pluto table INSERT (keep as SQL for now)
3. allocated UPDATE (~25 fields)
4. geocodes UPDATE (~20 fields)
5. 60+ targeted UPDATEs (Waves 1-4 targets)
6. DBT run → pluto_enriched
7. apply_dbt_enrichments → copy to pluto
8. Corrections override (separate script)
```

### Dependencies
- ✅ **No circular dependencies**
- ⚠️ **2 files must run before dbt:** latlong.sql, numericfields_geomfields.sql
- ✅ **7 fields updated multiple times** (analyzed and explained)
- ✅ **Clear migration path** identified

### Migration Waves
- **Wave 0:** pluto_rpad_geo (DO FIRST - enables targeted re-running)
- **Wave 1:** 7 simple files (CAMA, zoning, classification) - SAFE
- **Wave 2:** 2 files (separate tables)
- **Wave 3:** 3 files (pre-dbt dependencies) - CAREFUL
- **Wave 4:** 3+ misc files
- **Phase 5 (END GOAL):** Convert PLUTO itself to dbt model - pure dbt project

---

## 📝 Deliverables

### Documentation Files (4)
1. `dependencies.txt` - Full UPDATE statement dependency graph
2. `DATA_FLOW_ANALYSIS.md` - How PLUTO is built, data flow patterns
3. `OVERALL_STRATEGY.md` - Master migration strategy, what to migrate vs keep
4. `RPAD_GEO_DBT_ANALYSIS.md` - Wave 0 migration plan (pluto_rpad_geo)

### Analysis Scripts (2)
1. `analyze_dependencies.py` - Extracts UPDATE statements from SQL files
2. `analyze_migration_groups.py` - Groups files by migration priority

### Git Commits (5)
```
36fce9cc Add analysis: Should pluto_rpad_geo be a dbt model?
17a1cfbb Add comprehensive PLUTO dbt migration strategy
ed189b4a Add PLUTO data flow analysis and dbt migration strategy
89208385 Add execution order and inter-group dependency analysis
7f910379 Add PLUTO UPDATE statement dependency analysis
```

---

## 🚀 Recommended Next Actions

### Immediate (Wave 0)
1. **Create issue:** "DBT'ify pluto_rpad_geo as intermediate models"
   - Est: 2-3 hours
   - Creates: int__dof_pts_propmaster.sql, int__pluto_rpad_geo.sql
   - Removes: 7 SQL files
   - Enables: Targeted re-running with `dbt run --select int__pluto_rpad_geo+`

### Short-term (Wave 1)
2. **Create 7 issues** for simple file migrations:
   - CAMA: bsmttype, lottype, proxcode, easements (4 issues)
   - Zoning: splitzone, specialdistrict (2 issues)
   - Classification: bldgclass (1 issue)
   - All SAFE - no dependencies, can work in parallel

### Medium-term (Waves 2-4)
3. **Iterate through remaining files**
   - Wave 2: primebbl, yearbuiltalt
   - Wave 3: latlong, numericfields, dtmgeoms (verify dbt deps first)
   - Wave 4: plutomapid, versions, apdate

### Long-term (Phase 5)
4. **Plan pure dbt conversion**
   - Convert PLUTO table itself to dbt model
   - Eliminate all SQL scripts
   - Single `dbt build` command
   - Timeline: 9-12 months from start

---

## 💡 Key Insights for Future Work

### What Makes a Good Migration Candidate?
✅ Simple CASE statements or COALESCE logic
✅ Lookups from reference tables
✅ Calculations (e.g., FAR = bldgarea / lotarea)
✅ Single-pass transformations
✅ No spatial operations

### What Should Stay SQL Longer?
❌ Complex spatial operations (ST_AREA, ST_INTERSECTION with rankings)
❌ Progressive refinement (7 UPDATEs in same file)
❌ Corrections that override computed values
❌ Initial table creation and population

### The Hybrid Pattern Works Because:
1. SQL handles the procedural/imperative parts
2. DBT handles the declarative transformations
3. pluto_enriched acts as clean handoff point
4. Corrections can override without complex logic
5. Incremental migration path with low risk

### End Goal is Clear:
Pure dbt project where everything is declarative:
- All prep tables as dbt models
- PLUTO itself as dbt model
- Corrections as incremental model or post-hook
- Zero shell scripts, zero mutable tables
- Single `dbt build` command

**It's okay if this takes time.** The hybrid state is stable and provides value while we migrate incrementally.

---

## 📚 Reading Order for New Contributors

1. Start with: `OVERALL_STRATEGY.md` (big picture)
2. Then read: `DATA_FLOW_ANALYSIS.md` (how it works today)
3. For Wave 0: `RPAD_GEO_DBT_ANALYSIS.md` (first migration)
4. Reference: `dependencies.txt` (detailed analysis)

---

## ✅ Session Success

All objectives met:
- ✅ Reviewed open epic and current state
- ✅ Analyzed UPDATE statement dependencies
- ✅ Built dependency graph (products/pluto/dependencies.txt)
- ✅ Identified migration groups and order
- ✅ Answered: when are rows inserted? (bbl.sql line 22)
- ✅ Answered: should we dbt'ify pluto_rpad_geo? (YES - Wave 0)
- ✅ Answered: fields updated multiple times? (7 found, explained)
- ✅ Created comprehensive migration strategy
- ✅ Updated epic with documentation and end goal
- ✅ Established clear path forward

**The PLUTO dbt migration epic now has a complete strategy and roadmap. Ready to execute!** 🎯

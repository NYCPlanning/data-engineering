# PLUTO DBT Migration - Refined Plan

**Updated:** 2026-03-30
**Based on:** Plan review session with clarifications

---

## 🎯 Core Principles

1. **Priority:** Targeted re-running is the main goal → Early models more important
2. **Validation:** Compare final `{{ build_schema }}.pluto` vs `nightly_qa.pluto`
3. **Performance:** Must be equal or better than current
4. **Cleanup:** Rename SQL files to `{filename}_migrated.sql` after creating dbt models
5. **Scope:** Migrate ALL files (including complex spatial)
6. **Execution:** Solo work, fast iteration over careful validation
7. **Risk:** Accept some iterations, learn by doing

---

## 🔴 Known Issues

### latlong Performance Problem
- **History:** Previously migrated, hit performance issues
- **Status:** Model exists but commented out in pluto_enriched
- **Action:** Keep commented out for now, investigate in Wave 3
- **Workaround:** SQL version still runs (latlong.sql exists)

### flood_flag Not Integrated
- **Status:** Model exists, not in pluto_enriched or apply_dbt_enrichments
- **Action:** Add to integration in next convenient opportunity
- **Priority:** Low (not blocking other work)

---

## 📋 Execution Plan

### **Wave 0: pluto_rpad_geo** (DO FIRST - Highest Value) ⭐

**Why First:**
- Enables targeted re-running immediately
- Removes 7 SQL files in one shot
- High complexity = high learning value
- Sets foundation for all other prep tables

**Issue: "DBT'ify pluto_rpad_geo as intermediate models"**

**Creates:**
- `models/intermediate/prep/int__dof_pts_propmaster.sql`
- `models/intermediate/prep/int__pluto_rpad_geo.sql`

**Absorbs & Renames:**
1. create_pts.sql → create_pts_migrated.sql
2. create_rpad_geo.sql → create_rpad_geo_migrated.sql
3. zerovacantlots.sql → zerovacantlots_migrated.sql
4. lotarea.sql → lotarea_migrated.sql
5. primebbl.sql → primebbl_migrated.sql
6. apdate.sql → apdate_migrated.sql
7. geocode_billingbbl.sql → geocode_billingbbl_migrated.sql

**Updates:**
- 02_build.sh (replace 7 run_sql_file calls with dbt run)
- 9 downstream SQL files (reference int__pluto_rpad_geo instead)

**Validation:**
- Run full build
- Compare `build_schema.pluto` vs `nightly_qa.pluto`
- Spot check BBL counts, key fields

**Test Targeted Re-running:**
```bash
# Make change to int__pluto_rpad_geo
dbt run --select int__pluto_rpad_geo+
# Verify downstream rebuilt
```

**Estimated Time:** 3-4 hours (including testing)

---

### **Wave 1: Simple Business Logic** (7 Files)

**Do as 7 separate issues for fast iteration:**

#### **Issue 1.1: Migrate cama_bsmttype.sql**
- Creates: `models/intermediate/cama/int_cama__bsmttype.sql`
- Logic: `SET bsmtcode = '5' WHERE bsmtcode IS NULL`
- Rename: cama_bsmttype.sql → cama_bsmttype_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 30 min

#### **Issue 1.2: Migrate cama_lottype.sql**
- Creates: `models/intermediate/cama/int_cama__lottype.sql`
- Logic: `SET lottype = '0' WHERE lottype IS NULL`
- Rename: cama_lottype.sql → cama_lottype_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 30 min

#### **Issue 1.3: Migrate cama_proxcode.sql**
- Creates: `models/intermediate/cama/int_cama__proxcode.sql`
- Logic: `SET proxcode = '0' WHERE proxcode IS NULL`
- Rename: cama_proxcode.sql → cama_proxcode_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 30 min

#### **Issue 1.4: Migrate cama_easements.sql**
- Creates: `models/intermediate/cama/int_cama__easements.sql`
- Logic: `SET easements = '0' WHERE easements IS NULL`
- Rename: cama_easements.sql → cama_easements_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 30 min

#### **Issue 1.5: Migrate bldgclass.sql**
- Creates: `models/intermediate/simple/int_pluto__bldgclass.sql`
- Logic: `SET bldgclass = 'Q0' WHERE zonedist1 = 'PARK' AND (bldgclass IS NULL OR bldgclass LIKE 'V%')`
- Rename: bldgclass.sql → bldgclass_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 30 min

#### **Issue 1.6: Migrate zoning_splitzone.sql**
- Creates: `models/intermediate/zoning/int_zoning__splitzone.sql`
- Logic: Multiple UPDATEs → CTEs with progressive refinement
- Challenge: 6 UPDATEs to convert to declarative
- Rename: zoning_splitzone.sql → zoning_splitzone_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 1-2 hours

#### **Issue 1.7: Migrate zoning_specialdistrict.sql**
- Creates: `models/intermediate/zoning/int_zoning__specialdistrict.sql`
- Logic: Spatial joins with rankings → CTEs
- Challenge: 7 UPDATEs with complex spatial logic
- Rename: zoning_specialdistrict.sql → zoning_specialdistrict_migrated.sql
- Add to: pluto_enriched, apply_dbt_enrichments.sql
- Time: 2-3 hours

**Total Wave 1 Time:** ~7-10 hours

---

### **Wave 2: Separate Tables** (2 Files)

#### **Issue 2.1: Migrate create_allocated.sql**
- Creates: `models/intermediate/prep/int__pluto_allocated.sql`
- Challenge: Currently creates table via SELECT INTO with aggregations
- Convert to: dbt model that materializes pluto_allocated
- Rename: create_allocated.sql → create_allocated_migrated.sql
- Update: 02_build.sh, downstream SQL files
- Time: 2-3 hours

#### **Issue 2.2: Migrate yearbuiltalt.sql**
- Creates: `models/intermediate/prep/int__pluto_allocated_years.sql` OR
- Absorb into: int__pluto_allocated.sql (if Issue 2.1 done)
- Logic: Updates yearbuilt, yearalter1, yearalter2 on pluto_allocated
- Rename: yearbuiltalt.sql → yearbuiltalt_migrated.sql
- Time: 1-2 hours

**Note:** primebbl.sql likely absorbed by Wave 0 (pluto_rpad_geo)

**Total Wave 2 Time:** ~3-5 hours

---

### **Wave 3: Complex Files with Learnings** (Many Files)

**Approach:** Tackle after Waves 0-2 provide learning

**Categories:**

#### **3A: Geocoding (if not in Wave 0)**
- geocodes.sql - Major UPDATE with ~20 fields
- dtmgeoms.sql - Geometry operations  
- update_empty_coord.sql
- geocode_notgeocoded.sql

#### **3B: Spatial Operations**
- spatialjoins.sql - Political boundaries
- plutogeoms.sql - Geometry creation
- dtmmergepolygons.sql

#### **3C: Zoning (remaining)**
- zoning.sql - Complex logic with splitzone
- zoning_zoningdistrict.sql
- zoning_commercialoverlay.sql
- zoning_limitedheight.sql
- zoning_zonemap.sql
- zoning_parks.sql
- zoning_create_priority.sql

#### **3D: CAMA (remaining)**
- cama_bldgarea_1.sql through cama_bldgarea_4.sql
- create_cama_primebbl.sql (if not in Wave 0)

#### **3E: Address & Other**
- address.sql
- numericfields_geomfields.sql (pre-dbt dependency)
- plutomapid.sql, plutomapid_1.sql, plutomapid_2.sql
- versions.sql

**Strategy:** Create issues as we go, based on learnings from Waves 0-2

**Estimated:** 20-30 more files, ~30-40 hours

---

### **Wave 4: Backfill & Corrections**

#### **Issue 4.1: Handle backfill.sql**
- Status: Currently UNUSED (orphaned file)
- Action: Verify it's not called anywhere, then just DELETE
- Time: 15 min (verification)

#### **Issue 4.2: Handle corr_lotarea.sql & corrections**
- Status: Runs in 03_corrections.sh (separate script)
- Keep as SQL for now
- Defer to Phase 5 (corrections strategy)

**Total Wave 4 Time:** ~30 min

---

### **Phase 5: Pure DBT** (Future)

**Prerequisites:** All Waves 0-4 complete

**Major Changes:**
1. Convert PLUTO table itself to dbt model
2. Integrate corrections (incremental model or post-hook)
3. Eliminate 02_build.sh entirely
4. Single `dbt build` command

**Estimated:** 2-3 months of design + implementation

---

## 🎯 Practical Execution Strategy

### Per-Issue Workflow (Fast Iteration)

```bash
# 1. Create dbt model with header comment
cd products/pluto/models/intermediate/{category}/
# Write the model starting with:
# -- Migrated from: pluto_build/sql/{original_filename}.sql
# [rest of model logic...]

# 2. Test it in isolation
cd products/pluto
eval "$(direnv export bash)"
dbt run --select {model_name}

# 3. Add to pluto_enriched
# Edit models/product/pluto_enriched.sql
# Add LEFT JOIN

# 4. Add to apply_dbt_enrichments
# Edit pluto_build/sql/apply_dbt_enrichments.sql
# Add fields to SET clause

# 5. Update build script
# Edit pluto_build/02_build.sh
# Remove run_sql_file call

# 6. Rename original SQL file
mv pluto_build/sql/{original}.sql pluto_build/sql/{original}_migrated.sql

# 7. Test full build
cd pluto_build
./02_build.sh

# 8. Validate (spot check)
# Compare key fields, BBL counts between build_schema.pluto and nightly_qa.pluto

# 9. Commit
git add -A
git commit -m "Migrate {file} to dbt"

# 10. Move to next file (fast iteration!)
```

### Validation Approach (Quick & Practical)

**After each migration:**
```sql
-- Compare record counts
SELECT COUNT(*) FROM build_schema.pluto;
SELECT COUNT(*) FROM nightly_qa.pluto;

-- Spot check the migrated field(s)
SELECT bbl, {migrated_field}
FROM build_schema.pluto
WHERE {migrated_field} IS NOT NULL
LIMIT 100;

-- Compare with QA
SELECT 
    COUNT(*) as diff_count
FROM build_schema.pluto b
FULL OUTER JOIN nightly_qa.pluto q USING (bbl)
WHERE b.{migrated_field} IS DISTINCT FROM q.{migrated_field};
```

**After Wave complete:**
Full table comparison before moving to next wave.

### Performance Testing

**After Wave 0 (rpad_geo):**
- Time the full build
- Compare to current baseline
- If slower, investigate before proceeding

**After each wave:**
- Check build time hasn't degraded
- Optimize if needed

---

## 📊 Progress Tracking

### Metrics to Track

**Per Wave:**
- Files migrated
- SQL files renamed to *_migrated.sql
- Lines of SQL → dbt models
- Build time impact
- Issues encountered

**Overall:**
- Total files remaining: 70 → X
- Percentage migrated: 0% → 100%
- Time invested
- Time to targeted re-run

---

## 🚀 Immediate Next Steps

### This Week

**1. Create Wave 0 Issue**
```
Title: DBT'ify pluto_rpad_geo as intermediate models
Epic: de-74o
Priority: P0 (highest - enables targeted re-running)
Estimate: 4 hours
```

**2. Execute Wave 0**
- High complexity, high value
- Learn patterns for prep tables
- Validate targeted re-running works
- Performance test

**3. Document Learnings**
- What worked well?
- What was harder than expected?
- Refine approach for Wave 1

### Next 2-3 Weeks

**4. Create Wave 1 Issues (7 total)**
- Can create all at once since they're independent
- Tackle in any order
- Fast iteration (30 min - 3 hours each)

**5. Execute Wave 1**
- Build momentum
- Establish patterns for business logic
- Learn from complex spatial files (splitzone, specialdistrict)

**6. Validate After Wave 1**
- Full pluto table comparison
- Performance check
- Document patterns

### Next Month

**7. Wave 2 (Separate Tables)**
- pluto_allocated strategy
- Build on Wave 0 learnings

**8. Start Planning Wave 3**
- Identify next batch of files
- Prioritize by complexity/value
- Create issues

---

## 🎓 Learning Goals by Wave

### Wave 0: Prep Tables
- How to handle complex joins/dedup
- Multiple SQL files → single model
- Materialization strategies
- Targeted re-running patterns

### Wave 1: Business Logic
- Simple transformations (CAMA defaults)
- Complex progressive refinement (zoning)
- Spatial operations in dbt
- Performance considerations

### Wave 2: Table Creation
- Materializing non-PLUTO tables
- Aggregation patterns
- Downstream dependencies

### Wave 3: Everything Else
- Apply all learnings
- Tackle remaining complexity
- Refine patterns

### Phase 5: Pure DBT
- PLUTO as dbt model
- Corrections strategy
- Final architecture

---

## ✅ Success Criteria

### After Wave 0
- ✅ Can run: `dbt run --select int__pluto_rpad_geo+` 
- ✅ 7 SQL files renamed to *_migrated.sql
- ✅ Full build works
- ✅ Performance acceptable
- ✅ Targeted re-running validated

### After Wave 1
- ✅ 14 SQL files renamed (7 + Wave 0's 7)
- ✅ CAMA, zoning, classification in dbt
- ✅ Complex spatial files migrated
- ✅ Patterns established

### After Wave 2
- ✅ 16 SQL files renamed
- ✅ prep tables all in dbt
- ✅ pluto_allocated as dbt model

### After Wave 3
- ✅ ~40+ SQL files renamed
- ✅ Only corrections & edge cases remain
- ✅ Ready for Phase 5

### After Phase 5
- ✅ All SQL files migrated (*_migrated.sql as reference)
- ✅ `dbt build` is the entire build
- ✅ Pure dbt project achieved

---

## 🎯 The Path Forward

```
TODAY
  ↓
Wave 0: pluto_rpad_geo (4 hrs) → Targeted re-running unlocked ⭐
  ↓
Wave 1: Simple files (10 hrs) → Patterns established
  ↓
Wave 2: Separate tables (5 hrs) → Foundation complete
  ↓
Wave 3: Complex files (40 hrs) → Bulk migration
  ↓
Wave 4: Cleanup (1 hr) → Almost pure dbt
  ↓
Phase 5: Pure dbt (months) → END GOAL achieved
```

**Total estimated:** ~60 hours of migration work + Phase 5 design/implementation

**Timeline:** 
- Waves 0-2: 2-3 weeks
- Wave 3: 1-2 months  
- Wave 4: 1 day
- Phase 5: 3-6 months

**Let's start with Wave 0 and learn by doing!** 🚀


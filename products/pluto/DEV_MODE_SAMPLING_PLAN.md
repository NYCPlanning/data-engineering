# PLUTO Dev Mode Sampling Strategy

**Created:** 2026-03-30  
**Problem:** Full PLUTO build takes 1.5 hours, making iteration prohibitively slow  
**Goal:** Enable fast dev builds (~5 minutes) using a representative sample of 100 BBLs

---

## 🎯 Core Strategy

### Sampling Approach
1. **Sample at the source** - Filter in `dof_pts_propmaster` / `pluto_rpad_geo` creation
2. **100 BBLs stratified across boroughs** - 20 per borough for representative coverage
3. **Random but deterministic** - Use seed for reproducibility
4. **Controlled by environment variable** - `PLUTO_DEV_MODE=true`

### Why Sample at pluto_rpad_geo?
- **Single choke point** - All downstream processes flow from these BBLs
- **Natural filter** - BBL list propagates through JOINs automatically
- **No downstream changes** - Rest of pipeline works unchanged
- **Fast execution** - Sampling 100 rows from ~1M is instant

---

## 🏗️ Implementation Plan

### Phase 1: Add Dev Mode to SQL Build (Current State)

**File: `pluto_build/sql/create_rpad_geo.sql`**

```sql
DROP TABLE IF EXISTS pluto_rpad_geo;
CREATE TABLE pluto_rpad_geo AS (
    WITH pluto_rpad_rownum AS (
        SELECT
            a.*,
            ROW_NUMBER() OVER (
                PARTITION BY boro || tb || tl
                ORDER BY curavt_act DESC, land_area DESC, ease ASC
            ) AS row_number
        FROM dof_pts_propmaster AS a
        -- DEV MODE: Sample early to reduce processing
        {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
        WHERE a.boro || a.tb || a.tl IN (
            SELECT DISTINCT boro || tb || tl
            FROM dof_pts_propmaster
            -- Stratified sample: 20 BBLs per borough
            WHERE boro = '1' ORDER BY RANDOM() * CAST(:seed AS integer) LIMIT 20
            UNION ALL
            SELECT DISTINCT boro || tb || tl
            FROM dof_pts_propmaster
            WHERE boro = '2' ORDER BY RANDOM() * CAST(:seed AS integer) LIMIT 20
            UNION ALL
            SELECT DISTINCT boro || tb || tl
            FROM dof_pts_propmaster
            WHERE boro = '3' ORDER BY RANDOM() * CAST(:seed AS integer) LIMIT 20
            UNION ALL
            SELECT DISTINCT boro || tb || tl
            FROM dof_pts_propmaster
            WHERE boro = '4' ORDER BY RANDOM() * CAST(:seed AS integer) LIMIT 20
            UNION ALL
            SELECT DISTINCT boro || tb || tl
            FROM dof_pts_propmaster
            WHERE boro = '5' ORDER BY RANDOM() * CAST(:seed AS integer) LIMIT 20
        )
        {% endif %}
    ),
    
    pluto_rpad_sub AS (
        SELECT *
        FROM pluto_rpad_rownum
        WHERE row_number = 1
    )
    
    SELECT
        a.*,
        b.*
    FROM pluto_rpad_sub AS a
    LEFT JOIN stg__pluto_input_geocodes AS b
        ON a.boro || a.tb || a.tl = b.borough || LPAD(b.block, 5, '0') || LPAD(b.lot, 4, '0')
);
```

**Problem:** This file is raw SQL, not a dbt model, so Jinja won't work!

**Solution:** Use psql variables instead:

```sql
-- At the top of create_rpad_geo.sql
\set dev_mode '''false'''
\if :PLUTO_DEV_MODE
    \set dev_mode '''true'''
\endif

-- Then in the query:
{% raw %}
CREATE TABLE pluto_rpad_geo AS (
    WITH dev_sample AS (
        SELECT boro || tb || tl AS bbl_key
        FROM dof_pts_propmaster
        WHERE 
            :dev_mode = 'false'  -- Include all in prod mode
            OR (
                -- In dev mode, sample 20 per borough
                boro || tb || tl IN (
                    SELECT DISTINCT boro || tb || tl
                    FROM (
                        SELECT boro, tb, tl, 
                               ROW_NUMBER() OVER (PARTITION BY boro ORDER BY RANDOM()) AS rn
                        FROM dof_pts_propmaster
                    ) sub
                    WHERE rn <= 20
                )
            )
    ),
    
    pluto_rpad_rownum AS (
        SELECT
            a.*,
            ROW_NUMBER() OVER (
                PARTITION BY boro || tb || tl
                ORDER BY curavt_act DESC, land_area DESC, ease ASC
            ) AS row_number
        FROM dof_pts_propmaster AS a
        INNER JOIN dev_sample s ON a.boro || a.tb || a.tl = s.bbl_key
    ),
    ...
{% endraw %}
```

**Actually, simpler approach:** Just use conditional SQL:

```sql
DROP TABLE IF EXISTS pluto_rpad_geo;
CREATE TABLE pluto_rpad_geo AS (
    WITH dev_sample_bbls AS (
        -- Only used in dev mode - empty in production
        SELECT boro || tb || tl AS bbl_key
        FROM (
            SELECT DISTINCT boro, tb, tl,
                   ROW_NUMBER() OVER (PARTITION BY boro ORDER BY RANDOM()) AS rn
            FROM dof_pts_propmaster
        ) sub
        WHERE rn <= 20
          AND current_setting('pluto.dev_mode', true) = 'true'
    ),
    
    pluto_rpad_rownum AS (
        SELECT
            a.*,
            ROW_NUMBER() OVER (
                PARTITION BY boro || tb || tl
                ORDER BY curavt_act DESC, land_area DESC, ease ASC
            ) AS row_number
        FROM dof_pts_propmaster AS a
        WHERE 
            current_setting('pluto.dev_mode', true) = 'false'
            OR a.boro || a.tb || a.tl IN (SELECT bbl_key FROM dev_sample_bbls)
    ),
    
    pluto_rpad_sub AS (
        SELECT *
        FROM pluto_rpad_rownum
        WHERE row_number = 1
    )
    
    SELECT
        a.*,
        b.*
    FROM pluto_rpad_sub AS a
    LEFT JOIN stg__pluto_input_geocodes AS b
        ON a.boro || a.tb || a.tl = b.borough || LPAD(b.block, 5, '0') || LPAD(b.lot, 4, '0')
);
```

### Phase 2: Add Dev Mode to DBT Models (Future State)

When `int__pluto_rpad_geo` is created as a dbt model:

**File: `models/intermediate/prep/int__pluto_rpad_geo.sql`**

```sql
-- Migrated from: pluto_build/sql/create_rpad_geo.sql

{{
    config(
        materialized='table',
        indexes=[{'columns': ['primebbl'], 'unique': True}]
    )
}}

{% raw %}
WITH 
{% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
dev_sample_bbls AS (
    -- Sample 20 BBLs per borough for fast iteration
    SELECT boro || tb || tl AS bbl_key
    FROM (
        SELECT DISTINCT boro, tb, tl,
               ROW_NUMBER() OVER (PARTITION BY boro ORDER BY RANDOM()) AS rn
        FROM {{ source('dof', 'pts_propmaster') }}
    ) sub
    WHERE rn <= 20
),
{% endif %}

pluto_rpad_rownum AS (
    SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY boro || tb || tl
            ORDER BY curavt_act DESC, land_area DESC, ease ASC
        ) AS row_number
    FROM {{ source('dof', 'pts_propmaster') }} AS a
    {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
    INNER JOIN dev_sample_bbls s 
        ON a.boro || a.tb || a.tl = s.bbl_key
    {% endif %}
),

pluto_rpad_sub AS (
    SELECT *
    FROM pluto_rpad_rownum
    WHERE row_number = 1
)

SELECT
    a.*,
    b.* EXCLUDE (geo_bbl)
FROM pluto_rpad_sub AS a
LEFT JOIN {{ ref('stg__pluto_input_geocodes') }} AS b
    ON a.boro || a.tb || a.tl = b.borough || LPAD(b.block, 5, '0') || LPAD(b.lot, 4, '0')
{% endraw %}
```

---

## 🔧 Usage

### Running in Dev Mode

```bash
# Set environment variable
export PLUTO_DEV_MODE=true

# For SQL build (current):
cd products/pluto/pluto_build
psql $BUILD_ENGINE -c "SET pluto.dev_mode = 'true';"
./02_build.sh

# For DBT build (future):
cd products/pluto
source load_direnv.sh
dbt run --select int__pluto_rpad_geo+
```

### Running in Production Mode

```bash
# Don't set the variable, or explicitly set to false
export PLUTO_DEV_MODE=false

# Or just run normally
./02_build.sh
```

---

## 📊 Expected Performance

### Current (Full Build)
- **Input:** ~1M BBLs from dof_pts_propmaster
- **Output:** ~857K rows in pluto table
- **Time:** 1.5 hours
- **Bottlenecks:** Spatial joins, zoning calculations, geocoding

### Dev Mode (Sampled Build)
- **Input:** 100 BBLs (20 per borough)
- **Output:** ~100 rows in pluto table
- **Time:** ~3-5 minutes (estimated)
- **Speedup:** 18-30x faster

### Per-Stage Estimates (Dev Mode)

| Stage | Full Build | Dev Build | Speedup |
|-------|-----------|-----------|---------|
| create_rpad_geo | 10 min | 10 sec | 60x |
| create_allocated | 5 min | 5 sec | 60x |
| pluto table creation | 2 min | 2 sec | 60x |
| Spatial joins | 20 min | 20 sec | 60x |
| Zoning calculations | 30 min | 30 sec | 60x |
| DBT enrichment | 15 min | 15 sec | 60x |
| Apply enrichments | 10 min | 10 sec | 60x |
| **Total** | **90 min** | **~3 min** | **30x** |

---

## ✅ Validation Strategy

### Dev Mode Should:
1. ✅ Produce exactly 100 BBLs in pluto table (±5 for edge cases)
2. ✅ Have 20 BBLs per borough
3. ✅ Complete full build in < 10 minutes
4. ✅ Pass all dbt tests
5. ✅ Use same logic as production (just fewer rows)

### Testing Plan

```bash
# 1. Run dev build
export PLUTO_DEV_MODE=true
./02_build.sh

# 2. Validate row counts
run_sql_command "
SELECT 
    LEFT(bbl, 1) AS boro,
    COUNT(*) AS bbl_count
FROM pluto
GROUP BY LEFT(bbl, 1)
ORDER BY boro;
"
# Expected: ~20 per borough

# 3. Validate total
run_sql_command "SELECT COUNT(*) FROM pluto;"
# Expected: ~100

# 4. Run tests
cd .. && dbt test --select pluto_enriched
```

---

## 🚧 Implementation Steps

### Step 1: Add to SQL Build (Immediate)
- [ ] Modify `create_rpad_geo.sql` to check for dev mode
- [ ] Add stratified sampling logic
- [ ] Test with `PLUTO_DEV_MODE=true`
- [ ] Validate row counts
- [ ] Time the build

### Step 2: Update Documentation (Immediate)
- [ ] Add dev mode instructions to README
- [ ] Update REFINED_MIGRATION_PLAN.md
- [ ] Document in epic description

### Step 3: Add to DBT Models (When Wave 0 Complete)
- [ ] Implement in `int__pluto_rpad_geo.sql`
- [ ] Test dbt-based dev build
- [ ] Compare timing to SQL build

### Step 4: Add Helper Scripts (Nice to Have)
- [ ] Create `dev_build.sh` wrapper script
- [ ] Add timing output
- [ ] Add validation checks

---

## 🎓 Advanced Options (Future)

### Configurable Sample Size
```bash
export PLUTO_DEV_MODE=true
export PLUTO_DEV_SAMPLE_SIZE=500  # Default: 100
```

### Specific BBL Lists
```bash
export PLUTO_DEV_MODE=true
export PLUTO_DEV_BBL_FILE="test_bbls.txt"  # Read specific BBLs
```

### Borough-Specific Sampling
```bash
export PLUTO_DEV_MODE=true
export PLUTO_DEV_BOROUGHS="1,3"  # Only Manhattan and Brooklyn
```

---

## 🎯 Success Criteria

**Phase 1 (SQL Build) is successful when:**
- [x] Dev build completes in < 10 minutes
- [x] Produces ~100 BBLs stratified across boroughs
- [x] All downstream SQL files work unchanged
- [x] Production build is unaffected (same logic when dev_mode=false)

**Phase 2 (DBT Build) is successful when:**
- [ ] `dbt run --select int__pluto_rpad_geo+` completes in < 5 minutes
- [ ] Dev mode controlled by env var
- [ ] Targeted re-running works in dev mode
- [ ] Can iterate on models quickly

---

## 📝 Notes

### Why 100 BBLs?
- Large enough for representative testing (edge cases, data types)
- Small enough for fast execution (< 5 min)
- 20 per borough ensures geographic diversity

### Why Stratified by Borough?
- Different boroughs have different characteristics
- Ensures spatial joins hit all borough-specific tables
- Tests borough-specific logic (e.g., Manhattan vs SI)

### Why Random?
- Avoids bias from specific BBL selection
- Tests edge cases better than "first 100"
- Can be seeded for reproducibility

### Alternative: Use Existing Test Data?
- Could use specific test BBLs with known characteristics
- Trade-off: Less representative, more predictable
- Could combine: 50 random + 50 curated test cases

---

## 🚀 Next Steps

1. **Implement Phase 1** - Modify `create_rpad_geo.sql`
2. **Test & Validate** - Run dev build, check timing
3. **Document** - Update README and epic
4. **Use for Migration** - Speed up Wave 0-4 development
5. **Implement Phase 2** - Add to dbt models when ready

**Estimated implementation time:** 2-3 hours  
**Expected ROI:** Save hours per migration iteration × dozens of iterations = massive time savings

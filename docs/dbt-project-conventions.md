
# DBT Project Conventions

Standard conventions for all dbt projects in this repository.

## Directory Structure

Models are organized in four layers. New projects should use all four. Existing projects don't have a `core/` layer yet and don't need to be migrated.

| # | Layer | Prefixes | Names are | Who reads it |
|---|---|---|---|---|
| 1 | `staging/` | `stg_`, `stg_base__`, ... | internal | engineers |
| 2 | `intermediate/` | `int_` | internal | engineers |
| 3 | `core/` | `dim_`, `fact_`, `scd_`, `mart_`, `ref_` | internal, stable | engineers, internal analysts, `product/` |
| 4 | `product/` | none | public: the export filenames | the public, tools, destinations |

### `staging/`
Minimal transformations to recipe inputs or seeds:
- CRS projection changes
- Column selection/renaming to align with project conventions
- Type casting for consistency
- No business logic

### `intermediate/`
Business logic and transformations:
- `intermediate/simple/` - Single-purpose lookups and calculations (one file per concern)
- `intermediate/{topic}/` - Complex multi-table logic grouped by domain (e.g., `intermediate/cama/`, `intermediate/zoning/`)

### `core/`
Tables exposed to engineers and internal analysts. See [Data modeling](./data-modeling.md#core-shapes) for their shapes and rules.

### `product/`
Final tables ready for export, in one folder per product. Names match the exported files, so renaming a product model breaks its consumers. See [Data modeling](./data-modeling.md#product-tables) for what a product model can do.

## Model Configuration

## Adding New Models
When adding a new .sql file, also check whether you need to add an accompanying models yml file.

### Materialization
- `staging/`: `view` (default) unless indexes are required
- `intermediate/`: `view` by default. Set specific models to `table` (with appropriate indexes) when they're expensive to recompute or read by several downstream models.
- `core/`: `table` with appropriate indexes
- `product/`: `table` with appropriate indexes

### Indexing
Models joined on BBL require a unique BBL index:
```sql
{{ config(
    materialized='table',
    indexes=[{'columns': ['bbl'], 'unique': True}]
) }}
```

### Schema Tests
Add `schema.yml` with tests for all intermediate models:
```yaml
models:
  - name: int_example
    columns:
      - name: bbl
        tests:
          - unique
          - not_null
```

## Geometry Standards

- **Column name**: `geom` (not `wkb_geometry`)
- **Projection**: builds and exports use both EPSG:2263 (NY State Plane) and EPSG:4326 (WGS84), depending on the product. Measure areas and distances in 2263.

## Linting

Run sqlfluff from repository root:
```bash
sqlfluff lint --dialect postgres --templater jinja <path>
sqlfluff fix --dialect postgres --templater jinja <path>
``` 



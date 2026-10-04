
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
The clean model of the data, built only from `select` and joins of `intermediate/` models. All logic stays in `intermediate/`. One model per grain:
- `dim_`: one row per thing that persists (a lot, a building, an application)
- `fact_`: one row per thing that happened (a filing, a permit issuance)
- `scd_`: one row per version of an entity's attributes, with `valid_from` / `valid_to`
- `mart_`: one row per combination of things (e.g. community district by year)
- `ref_`: one row per code in a code list, with its label

### `product/`
Final tables ready for export, in one folder per product. Names match the exported files, so renaming a product model breaks its consumers.

Product models shape `core/` models for a destination or tool. They can filter rows, select and rename columns, choose a geometry column, and cast types. They don't compute anything new: if a product model couldn't be generated from a config file, its logic belongs in `intermediate/`.

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



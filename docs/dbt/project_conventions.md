# DBT Project Conventions

Standard conventions for all dbt projects in this repository.

## Directory Structure

### `staging/`
Minimal transformations to recipe inputs or seeds:
- CRS projection changes
- Column selection/renaming to align with project conventions
- Type casting for consistency
- No business logic

### `intermediate/`
Core business logic and transformations:
- `intermediate/simple/` - Single-purpose lookups and calculations (one file per concern)
- `intermediate/{topic}/` - Complex multi-table logic grouped by domain (e.g., `intermediate/cama/`, `intermediate/zoning/`)

### `product/`
Final tables ready for export.

## Model Configuration

### Materialization
- `staging/`: `view` (default) unless indexes are required
- `intermediate/`: `table` with appropriate indexes
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
- **Projection**: EPSG:2263 (NY State Plane) for build-time, EPSG:4326 (WGS84) only for export

## Linting

Run sqlfluff from repository root:
```bash
sqlfluff lint --dialect postgres --templater jinja <path>
sqlfluff fix --dialect postgres --templater jinja <path>
``` 



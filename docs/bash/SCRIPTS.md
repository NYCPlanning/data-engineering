# Bash Scripts & CLI Tools

Available in `bash/bin/`. Most are on PATH after setup.

## Recipe Management

**Load a recipe into build engine:**
```bash
dcp_load_recipe <product_name>
```

**Load a planned recipe:**
```bash
dcp_load_planned_recipe <product_name>
```

## Build Operations

**Trigger a build:**
```bash
dcp_trigger_build <dataset_name>
```

**Distribute outputs:**
```bash
dcp_distribute <dataset_name>
```

## Database Operations

**Run SQL query:**
```bash
run_sql_command "SELECT * FROM table"
```
Uses `BUILD_ENGINE_SERVER` env var (postgres connection string).

**Run SQL from file:**
```bash
run_sql_file path/to/query.sql
```

## Utilities

**Export shapefile:**
```bash
shp_export <options>
```

**Load direnv (for products):**
```bash
source load_direnv.sh
```

## Usage Pattern

For ease of use, use direnv when working in `products/*`:
```bash
eval "$(direnv export bash)" && dcp_load_recipe pluto
```

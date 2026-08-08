# Melissa Geocoding

Geocodes NYC addresses from Melissa Data's vendor file against NYC Geosupport
(Functions 1E, 1A, and AP), producing BBL, BIN, coordinates, community
district, NTA, and address point data for each address. Ported from the
Postgres-based [db-melissa](https://github.com/NYCPlanning/db-melissa)
pipeline onto DuckDB -- see `pipeline.py`'s module docstring for the mapping
between this code and the original SQL files.

## Running it

- **`./run_e2e.sh [output.csv] [limit]`** -- the full pipeline, sourcing
  `melissa_input` from `edm-private` via `recipe.yml`/`dcp_load_recipe`. This
  is the normal way to run it. Pass a `limit` to only process the first N rows
  of `melissa_input`, for a cheap sanity check before a full (~4M row) run.
- **`./run_geocode.sh`** / **`./run_geocode_automated.sh <s3-url>`** -- run
  against a local pipe-delimited file or one fetched ad hoc from an S3 URL,
  bypassing the recipe entirely. See `README_AUTOMATED.md`.
- **`.github/workflows/melissa_geocode.yml`** -- the same `run_e2e.sh` flow as
  a manually-triggered GitHub Action.

Either way, the real work (`pipeline.py`) needs `python-geosupport`, which
only exists inside the `nycplanning/build-geosupport` Docker image -- it is
**not** installed in this repo's own `.venv`.

## Data flow

Three sources land in DuckDB, via two different loading paths, before any
Python processing happens:

- **`melissa_input`** (the ~4.35M-row vendor file) and **`dcp_nta2020`** (NYC's
  2020 NTA reference dataset, code -> name) are pulled from `edm-private` /
  `edm-recipes` by `dcp_load_recipe` (on the host, since it needs this repo's
  `dcpy` package) and loaded straight into DuckDB tables via `read_csv_auto`
  / `read_parquet` -- a bulk, set-oriented load, no row-by-row Python.
- **`melissa_corrections`** / **`melissa_outsideofnyc`** (small local CSVs
  checked into `data/`) are read with `pandas.read_csv` in `pipeline.py`'s
  `_load_csv_table`, then registered into DuckDB as ordinary tables. Pandas is
  just acting as a CSV parser here.

From there, `pipeline.py` runs a sequence of stages, alternating between pure
DuckDB SQL and Python:

1. **`normalize_column_case`** (DuckDB only) -- fixes the mixed-case columns
   the recipe loader preserves from the CSV header (`Address` -> `address`).
2. **`preprocess`** (DuckDB only) -- computes `id = address || city || zip` on
   all three tables, deletes `melissa_input` rows matching
   `melissa_outsideofnyc`.
3. **`geocode_stage`** (DuckDB <-> Python <-> Geosupport) -- the one place
   data actually leaves DuckDB and is processed row by row, because
   Geosupport is a C library only reachable through the `python-geosupport`
   bindings; there's no SQL equivalent. For each of `melissa_input` and
   `melissa_corrections`, per chunk (`--chunk-size`, default 5000):
   - **DuckDB -> Python**: pull the not-yet-geocoded distinct
     `(id, address, zip_code)` tuples (anti-joined against the checkpoint
     table) out as a list of plain dicts.
   - **Python -> Geosupport, in parallel**: `multiprocessing.Pool.map` spreads
     the chunk across CPU cores; each worker calls `geocoding.py`'s
     `geocode()`, which parses the address (`usaddress`) and calls Geosupport
     Functions 1E, 1A, and AP -- the actual C-library calls.
   - **Python -> DuckDB**: the chunk's results become a `pandas.DataFrame` and
     get `INSERT`ed into `melissa_input_geocode` / `melissa_corrections_geocode`,
     committed immediately. That table *is* the checkpoint: rerunning the same
     command re-evaluates the anti-join and only geocodes what's still
     missing, so a crash loses at most one chunk.
4. **`build`** (DuckDB only) -- one `COALESCE`-based join across the geocode
   tables and `melissa_corrections`, producing one row per distinct address
   (corrections preferred over the original geocode result wherever both
   exist).
5. **`fill_nta_names`** (DuckDB only) -- a plain `UPDATE ... FROM dcp_nta2020`
   join, keyed on the 2020 NTA code Geosupport returns (Geosupport itself
   doesn't pair a name with that code -- see `geocoding.py`'s `parse_1e`).
6. **`output`** (DuckDB only) -- re-joins the per-address results back onto
   every original `melissa_input` row (restoring suite-level granularity) and
   writes the final CSV directly with `COPY ... TO`.

So structurally: DuckDB handles all the set-based work (loading, joins,
dedup, coalescing, the final write), and Python is invoked for exactly one
thing -- calling the Geosupport C library per address -- with DuckDB tables as
the handoff point on both sides.

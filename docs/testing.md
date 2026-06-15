# Test Strategy

How tests are organized across the repo, how to run them, and the conventions we hold.

## Suites

| Suite | Location | Needs |
|---|---|---|
| **dcpy unit** | `dcpy/test/` | Mocked externals (`moto` for S3/AWS); a live Postgres in CI |
| **dcpy library** | `dcpy/test/library/` | Run **separately** — gdal + pyarrow conflict (parquet read/write fails after importing gdal) |
| **dcpy integration** | `dcpy/test_integration/` | Live infrastructure (Postgres, SFTP) |
| **product / app** | `products/*`, `apps/qa/` | Per-product; matrix-driven |

### dcpy unit tests (`dcpy/test/`)

Mirror the package tree (`connectors/`, `lifecycle/`, `models/`, `utils/`, …). Externals are
mocked — S3/AWS via `moto` (`mock_aws` in [`dcpy/test/conftest.py`](../dcpy/test/conftest.py),
which also sets `RECIPES_BUCKET` / `PUBLISHING_BUCKET`). Fixtures are layered through per-area
`conftest.py` files (ingest, builds, package, product_metadata, …).

Run them from the repo root:

```bash
uv run python -m pytest dcpy/test
```

> [!NOTE]
> **Local `--noconftest` gotcha.** `dcpy/test/conftest.py` imports `moto` at module load. If you
> haven't installed the test extras locally, collection fails; `uv run python -m pytest dcpy/test
> --noconftest` is the workaround. In CI the test deps are installed, so this isn't needed there.

The **product-metadata tests** (`dcpy/test/product_metadata/`) additionally need a local clone of
the product-metadata repo via `PRODUCT_METADATA_REPO_PATH` — see
[dcpy/README.md → Testing](./dcpy/README.md#testing).

### dcpy integration tests (`dcpy/test_integration/`)

Require real services (Postgres, SFTP). CI runs these inside the dev container stack (`de`,
`postgis`, `sftp-server`) started via `docker compose`. To run them locally, start the dev
container and run:

```bash
python3 -m pytest dcpy/test_integration -v -s
```


### Product / app suites

Defined as a matrix in [`.github/workflows/data/pytest.yml`](../.github/workflows/data/pytest.yml)
(e.g. `checkbook`, `zap`, `qa`), each a `pytest` invocation run by `test_helper.yml`.

## Conventions

- **Coverage:** branch coverage on `dcpy` (`[tool.coverage.run]` in `pyproject.toml`), uploaded to
  Codecov. `dcpy/utils` (the foundation) targets **>90%** — alter with care.
- **Strict markers:** `addopts = "--strict-markers"` — every marker must be declared in
  `[tool.pytest.ini_options].markers`. The only custom marker is `end_to_end`, used to split fast
  vs. slow runs in the **`zap` product suite** (`-m 'not end_to_end'` / `-m 'end_to_end'`); it is
  declared but unused inside `dcpy/test`.
- **`xfail_strict = true`:** an `xfail` that unexpectedly passes is a failure — keep xfails honest.
- **New processing functions need a test.** When you add an ingest processing step or similar
  reusable function, add a unit test for it (see the
  [library → ingest migration guide](./dcpy/library-to-ingest-migration.md)).
- **Don't introduce flaky tests** into `dcpy/test` or `dcpy/test_integration`.

## How CI runs the dcpy suite

`test_helper.yml` (job `pytest_dcpy`) runs inside the `de` dev container with `postgis` and
`sftp-server` services, after cloning product-metadata. Roughly:

```bash
# main unit suite (library excluded), with coverage
python3 -m pytest dcpy/test --ignore dcpy/test/library --cov-config=pyproject.toml --cov=dcpy --cov-report=xml
# library suite, separately (gdal/pyarrow conflict), appending coverage
python3 -m pytest dcpy/test/library --cov=dcpy --cov-report=xml --cov-append
# integration suite
python3 -m pytest dcpy/test_integration ...
```

## Known gaps

- The `end_to_end` marker is declared globally but only exercised by product suites — consider
  documenting or scoping it if more suites adopt it.

# Test Strategy

How tests are organized across the repo, how to run them, and the conventions we hold.

## Suites

| Suite | Location | Needs |
|---|---|---|
| **dcpy unit (per package)** | `python/*/test/` | Mocked externals (`moto` for S3/AWS); each package synced **in isolation** in CI — see [Isolated packages](#isolated-packages-package-boundaries) |
| **dcpy library** | `python/library/test/` | Run **separately** — gdal + pyarrow conflict (parquet read/write fails after importing gdal) |
| **dcpy integration** | `dcpy/test_integration/` | Live infrastructure (Postgres, SFTP, S3); needs the **whole workspace** (`uv sync --all-packages`) — spans packages by design |
| **dcpy integration library** | `dcpy/test_integration/library/` | Live S3 + Postgres; run **separately** for the same gdal/pyarrow reason |
| **product / app** | `products/*`, `apps/qa/` | Per-product; matrix-driven |

### dcpy unit tests (`python/*/test/`)

dcpy is a uv workspace: each package under `python/*/` (`dcpy-utils`, `dcpy-connectors`,
`dcpy-lifecycle`, …) has its own `test/` directory mirroring its own source tree, its own
`conftest.py`, and its own `[dependency-groups] dev` with exactly the test deps it needs. Externals
are mocked — S3/AWS via `moto` (`mock_aws`, set up per package's `conftest.py`, which also sets
`RECIPES_BUCKET` / `PUBLISHING_BUCKET`).

For day-to-day local work, sync the whole workspace once and run any package's tests against it:

```bash
uv sync --all-packages
uv run --no-sync pytest python/lifecycle/test
```

That's convenient, but it's also permissive: with every package installed into one shared venv, a
package's tests can pass by accidentally importing a sibling that was never declared as a real
dependency. See [Isolated packages](#isolated-packages-package-boundaries) below for how CI catches
that for real.

The **product-metadata tests** (`python/product-metadata/test/`) read from the in-repo
`product-metadata/` directory by default — no extra setup required. See
[dcpy/README.md → Testing](./dcpy/README.md#testing).

### Isolated packages (package boundaries)

The package split itself is what's enforced — there's no separate import-linter config to run.
Each package declares the dcpy packages it actually depends on in its own `pyproject.toml`, and
`uv sync --package dcpy-X` installs **only** `X` and its declared dependencies, pruning everything
else out of the venv. Run that way, an import that reaches a package `X` doesn't depend on fails
outright (`ModuleNotFoundError`) instead of merely triggering a lint warning — that's the boundary
check. Each package also has a `test/test_package_boundary.py` that asserts the packages above it
in the hierarchy raise `ImportError` when synced this way.

Reproduce it locally with:

```bash
bash/run_isolated_tests.sh
```

which syncs and tests each `python/*/` package one at a time, the same way CI's `pytest_dcpy` job
does (see [How CI runs the dcpy suite](#how-ci-runs-the-dcpy-suite) below). **CI will catch a
mistaken boundary import** — a package importing a sibling it doesn't declare as a dependency — as
a test-collection failure on that package's isolated sync, not as a separate lint step. See
[dcpy architecture → Enforcement](./dcpy/architecture.md#enforcement) for the full mechanics and the
currently-known, `xfail`-tracked exceptions.

### dcpy integration tests (`dcpy/test_integration/`)

Require real services (Postgres, SFTP). Unlike the per-package unit suites, this one spans
multiple dcpy packages by design and isn't owned by any single one, so it stayed at its original
repo-root location and needs the full workspace (`uv sync --all-packages`) rather than an isolated
sync. CI runs it inside the dev container stack (`de`, `postgis`, `sftp-server`) started via
`docker compose`. To run it locally, start the dev container and run:

```bash
uv sync --all-packages
uv run --no-sync pytest dcpy/test_integration -v -s
```


### Product / app suites

Defined as a matrix in [`.github/workflows/data/pytest.yml`](../.github/workflows/data/pytest.yml)
(e.g. `checkbook`, `zap`, `qa`), each a `pytest` invocation run by `test_helper.yml`.

## Conventions

- **Coverage:** branch coverage measured **per package** (`--cov=python/<pkg>`, own
  `COVERAGE_FILE`, own `coverage-<pkg>.xml`), each uploaded to Codecov under its own `dcpy-<pkg>`
  flag — Codecov aggregates those into an overall total automatically. Scoping by package (rather
  than the whole `dcpy` namespace) keeps "covered by this package's own tests" from blending with
  "incidentally executed by a downstream consumer's tests". `dcpy-utils` (a foundation) targets
  **>90%** — alter with care.
- **Strict markers:** `addopts = "--strict-markers"` — every marker must be declared in
  `[tool.pytest.ini_options].markers`. The only custom marker is `end_to_end`, used to split fast
  vs. slow runs in the **`template` product suite** (`-m 'not end_to_end'` / `-m 'end_to_end'`); it
  is declared but unused inside `python/*/test`.
- **`xfail_strict = true`:** an `xfail` that unexpectedly passes is a failure — keep xfails honest.
  This is also how the known package-boundary deviations are tracked (see
  [Isolated packages](#isolated-packages-package-boundaries) above): an `xfail`-marked
  `test_package_boundary.py` case that starts passing means the coupling was fixed and the marker
  should come off.
- **New processing functions need a test.** When you add an ingest processing step or similar
  reusable function, add a unit test for it (see the
  [library → ingest migration guide](./dcpy/library-to-ingest-migration.md)).
- **Don't introduce flaky tests** into `python/*/test` or `dcpy/test_integration`.

## How CI runs the dcpy suite

`test_helper.yml` (job `pytest_dcpy`) runs inside the `de` dev container with `postgis` and
`sftp-server` services. Roughly:

```bash
# main unit suite: each package synced and tested in isolation (uv sync --package dcpy-<pkg>),
# one invocation per package — connectors and product-metadata sync dcpy-lifecycle alongside them
# too, to cover the known deviations (see dcpy/architecture.md#enforcement)
for pkg in connectors data geospatial lifecycle product-metadata utils; do
  uv sync --package "dcpy-$pkg" [--package dcpy-lifecycle]  # for connectors, product-metadata
  uv run --no-sync pytest "python/$pkg/test" --cov="python/$pkg" --cov-report="xml:coverage-$pkg.xml"
done

# library suite, separately (gdal/pyarrow conflict)
uv sync --package dcpy-library
uv run --no-sync pytest python/library/test --cov=python/library --cov-report=xml:coverage-library.xml

# integration suite: needs the whole workspace, since it spans packages by design
uv sync --all-packages
uv run --no-sync pytest dcpy/test_integration --ignore dcpy/test_integration/library ...
# integration library suite, separately (gdal/pyarrow again)
uv run --no-sync pytest dcpy/test_integration/library ...
```

> [!NOTE]
> Every package's `test/conftest.py` carries the same internet guard (`ensure_no_callouts`), which
> only replaces Python's `socket.socket`, so it can't see connections opened from C extensions —
> libpq, gdal. A unit test reaching live infrastructure through those will pass the guard; keep
> such tests in `dcpy/test_integration/` rather than relying on the guard to catch them.

## Known gaps

- The `end_to_end` marker is declared globally but only exercised by product suites — consider
  documenting or scoping it if more suites adopt it.

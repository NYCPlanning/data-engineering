# Local Development

First-time onboarding (GitHub access, SSH keys, 1Password, recommended tools, and the VSCode
dev-container walkthrough) lives on the [Developer Setup wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Developer-Setup).
The **dev container is the recommended way to develop**; this doc covers the command-driven
**manual setup** and day-to-day **Python dependency management**.

## Manual environment setup (uv)

Outside of a dev container, we use [uv](https://docs.astral.sh/uv/) for Python version and
package management.

### Install uv

- macOS and Linux
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- Windows (new DCP laptops)
  ```bash
  winget install --id=astral-sh.uv  -e
  ```
- Windows (old DCP desktop PCs)
  ```bash
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```

> [!NOTE]
> If prompted by uv, run `uv tool update-shell` to ensure the `PATH` includes necessary files.

### Manage Python versions

> [!WARNING]
> Without installing uv-managed Python interpreters, uv will use any interpreters it finds on the machine. This may cause issues, especially on Windows.

```bash
uv python list
uv python install 3.13
```

### Install system dependencies

#### Mac

With homebrew, install:
- `gdal` - the same version pinned in `python/library/pyproject.toml` (`GDAL==...`, which must exactly
  match the system libgdal used to build `nycplanning/base` - see `gdal_version` in
  `admin/run_environment/docker/base/setup.sh`) if possible. If not, edit that pin to align with the
  version returned from running `gdalinfo --version`, then run `uv lock`.
- postgres (latest version)

#### Windows

With **Conda** (Miniconda/Anaconda) in a **Git Bash** terminal, install `gdal` from the conda-forge channel to ensure PostGIS support:

```bash
conda install -c conda-forge gdal libgdal libgdal-pg
```

Install [PostgreSQL](https://www.postgresql.org/download/windows/) using the latest Windows installer.

*(Optional)* — expose the PostgreSQL CLI tools to your shell:

```bash
echo 'export PATH="$PATH:/c/Program Files/PostgreSQL/17/bin"' >> ~/.bashrc
source ~/.bashrc
```

Fix psql encoding (UTF-8) — the Windows psql client defaults to WIN1252, but our dumps are UTF-8:

```bash
# Option 1 — shell-level (add to ~/.bashrc or ~/.bash_profile)
export PGCLIENTENCODING=UTF8
```

```bash
-- Option 2 — psql-level (add to ~/.psqlrc)
SET client_encoding = 'UTF8';
```

### Set up a virtual environment

- macOS and Linux
  ```bash
  uv venv --python 3.13
  source .venv/bin/activate
  ```
- Windows
  ```bash
  uv venv --python 3.13
  .venv\Scripts\activate
  ```

### Install this repo's packages

`dcpy` is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) (each
`python/*` directory, plus `apps/qa`, `apps/dagster`, `apps/notebook-server`, is its own workspace
member with its own `pyproject.toml`); the whole workspace resolves against one committed
`uv.lock` at the repo root, which is the single source of truth for CI and local dev alike:

```bash
uv sync --all-packages
```

This installs every dcpy-* package editable, plus every workspace member's own dependencies, into
`.venv`. Run commands through the direnv-activated `.venv` (plain `python`, `pytest`, `dcpy`, …).
If you want `uv run`, it must be `uv run --no-sync --all-packages` to avoid re-syncing to a
narrower package set than the full workspace.

Product scripts under `products/` (not part of the uv workspace) and notebooks under `notebooks/`
sometimes need packages that aren't a dcpy dependency - those live in the root `pyproject.toml`'s
`products` extra:

```bash
uv sync --all-packages --extra products
```

## Loading environment variables (direnv)

The repo uses [direnv](https://direnv.net/) at every level. The root `.envrc` loads `.env`,
activates `.venv`, and adds `bash/bin` to `PATH`; each product's `.envrc` calls `source_up`
(inheriting the root setup) and sets product-specific vars like `BUILD_ENGINE_SCHEMA`. With direnv
installed and `direnv allow` run once per checkout, this happens automatically on `cd`.

In a non-interactive shell (scripts, some tooling) the hook doesn't fire — load it explicitly:

```bash
eval "$(direnv export bash)" && <command>
# or the convenience wrapper on PATH (bash/bin):
source load_direnv.sh && <command>
```

This matters most under `products/*` — without it, product-specific vars are missing and commands
fail or run with the wrong configuration. Without direnv installed at all, load the root env
manually:

```bash
source .venv/bin/activate && export $(cat .env | sed 's/#.*//g' | xargs)
```

## Managing Python dependencies

### Adding a package

1. Add the package to the `dependencies` (or a `[dependency-groups]`/`[project.optional-dependencies]`
   entry, for dev-only or non-dcpy-code needs) of whichever `pyproject.toml` actually needs it:
   - A dcpy layer's own runtime need → that package's `python/<name>/pyproject.toml`
     (e.g. `python/lifecycle/pyproject.toml` for something `dcpy.lifecycle` uses).
   - Dev/CI tooling (linters, test runners) not imported by any package → the root
     `pyproject.toml`'s `[dependency-groups] dev`.
   - A dependency of a one-off script under `products/` or `notebooks/` that isn't a dcpy
     dependency → the root `pyproject.toml`'s `products` extra.
   - An app (`apps/qa`, `apps/notebook-server`, `apps/dagster`) → that app's own `pyproject.toml`.
2. Run `uv lock` from the repo root and commit the updated `uv.lock` alongside the `pyproject.toml`
   change.
3. Run `uv sync --all-packages` (add `--extra products` if you touched that extra) to update your
   local `.venv`.

No other action is needed for CI or Docker images: they all export from the same committed
`uv.lock` (`uv export --package <name> ...`, see `admin/ops/docker_build_and_publish.sh`), so a
locked version bump here is picked up automatically the next time each image is built.

## Running a build locally

Examples mirror `.github/workflows/template_build.yml`:

```bash
# plan a build
python -m dcpy.lifecycle.builds.plan recipe
# load source data for a recipe
python -m dcpy lifecycle builds load load --recipe-path products/template/my_recipe.lock.yml
# run the transform step (from the product directory, e.g. products/template/)
python -m build_scripts.transform
```

## Secrets & database access

Local credentials and runtime config live in `.env` (loaded by `bash/bin/export_recipe_env.sh` and
by direnv). **Don't commit secrets** — CI uses 1Password and GitHub Secrets.

For SQL, prefer `run_sql_command` (on `PATH` via `bash/bin`). Otherwise use `BUILD_ENGINE_SERVER`, a
Postgres connection string of the form `postgresql://{user}:{password}@{host}:{port}`.

## Apps & notebooks

- Run the app stack locally: `cd apps && ./scripts/local-start.sh` (needs env vars from root `.env`).
- Marimo notebooks live in `notebooks/marimo/`, organized by `lifecycle/` and `products/`.

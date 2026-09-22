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
- `gdal` - the same version as in `admin/run_environment/requirements.txt` if possible. If not, edit the `gdal` version in that file to align with the version returned from running `gdalinfo --version`
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

```bash
# Third-party tooling that sits outside the dcpy workspace (mypy, and everything else baked
# into the nycplanning/dev image), pinned for reproducibility with CI
python -m pip install --requirement ./admin/run_environment/requirements.txt

# The dcpy workspace itself — every dcpy-* package under python/*/, plus dbt/sqlfluff/pytest —
# synced against the committed uv.lock, the same one CI syncs against
uv sync --all-packages
```

> [!NOTE]
> A `uv venv` environment has no `pip` module, so outside an activated venv (e.g. in a git
> worktree) use `uv pip install --requirement ...` instead of `python -m pip install ...`.

`uv sync --all-packages` is the standard way to install and update the dcpy workspace — re-run it
after pulling changes that touch any `python/*/pyproject.toml` or `uv.lock`. Prefer `uv run
--no-sync <command>` for one-off commands over a bare `uv run`, which re-syncs the venv on every
invocation — harmless, but pointlessly slow to repeat.

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

### Adding a dependency to a dcpy package

1. Add the package to the relevant `python/<pkg>/pyproject.toml`'s `dependencies` (or
   `[dependency-groups] dev` for a test-only dependency).
2. Run `uv sync --all-packages` — this re-resolves and updates the committed `uv.lock` to match,
   and installs the new package into your venv.
3. Commit both the `pyproject.toml` change and the updated `uv.lock`.

This is separate from `admin/run_environment/requirements.in` below — that file pins tooling that
sits outside the dcpy workspace, like `mypy` (see `bash/run_mypy.sh`) and whatever's baked into
the `nycplanning/dev` image; it isn't dcpy's own dependency list, so a new dcpy-only dependency
doesn't belong there.

### General uv workflow (`admin/run_environment/`)

For the repo-wide tooling pinned in `admin/run_environment/` (not the dcpy workspace — see above),
our preferred workflow when adding or updating packages is:
1. List required packages in a `requirements.in`
2. Compile them to a pinned `requirements.txt` via uv
3. Install from `requirements.txt`

```bash
# Compile
uv pip compile requirements.in --output-file requirements.txt

# Install
uv pip sync requirements.txt
```

Use the `--upgrade` flag to update pinned versions:

```bash
uv pip compile --upgrade requirements.in --output-file requirements.txt
```

Confirm the active environment and its packages:

```bash
which python
uv pip list
```

Do check in `requirements.in` and `requirements.txt`. Don't check in your virtual environment — make sure the folder is in `.gitignore`.

### Updating `admin/run_environment/` packages

1. Be up-to-date with `main`. If you have a long-running PR with merge conflicts in requirement files, it's far easier to take latest `main`, add new packages, and recompile.
2. Add the package(s) to `admin/run_environment/requirements.in`.
3. Run `admin/ops/python_compile_requirements.sh --no-upgrade`.

The `--no-upgrade` flag resolves the new package against the existing pinned versions in
`admin/run_environment/requirements.txt`, which can conflict. If so, run
`admin/ops/python_compile_requirements.sh` without the flag — it may upgrade unrelated packages
(usually fine). You can also selectively pin packages in `requirements.in` for the compile, then
unpin afterwards.

No other action is needed for CI: PR tests build a new image and run tests in it. Locally, either
update your venv with the regenerated `requirements.txt`, or once CI has run, build your dev
container from the `nycplanning/dev:dev-{branch}` image produced by tests.

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

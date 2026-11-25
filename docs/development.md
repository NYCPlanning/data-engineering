# Local Development

Getting access (GitHub, DigitalOcean, 1Password) and optional tools are on the
[Developer Setup wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Developer-Setup).

There are two ways to set up: the [dev container](#dev-container) (recommended) or a
[manual setup](#manual-environment-setup-uv) with uv. Either way, start by copying `example.env`
to `.env` in the repo root and filling in the values. Most of them are in 1Password.

## Dev container

The dev container is a Docker environment that VS Code connects to. CI runs the dcpy tests in the
same image, so it's the closest match to CI, though it can be slower than working outside a
container. It needs [Docker](https://docs.docker.com/get-docker/) and VS Code's Dev Containers
extension.

Its config is in `.devcontainer/`:

- `docker-compose.yml` defines the `de` container, plus a local `postgis` database and an
  `sftp-server` that the integration tests use. It loads `.env` and mounts the repo into `de`.
- `Dockerfile` builds `de` from our `dev` image (see [Docker images](./docker-images.md)).
- `devcontainer.json` is VS Code's config: extensions, forwarded ports, and
  `bash/docker_container_setup.sh`, which runs after the container is created.

To start it from VS Code, open the repo and pick "Reopen in Container". VS Code runs `ssh-add`
first, so it may ask for your SSH key's passphrase.

To start it from a terminal, run `docker compose up -d` in `.devcontainer/`, then
`docker exec -ti de bash` for a shell. This doesn't run `bash/docker_container_setup.sh`, so run
`./bash/docker_container_setup.sh` inside the container the first time to create `.venv`.

Use one route or the other. The container names are fixed (`de`, `postgis-db`), so if you started
it from a terminal, "Reopen in Container" fails with "container name ... is already in use". Run
`docker compose down` in `.devcontainer/` first.

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
- `gdal`, for the `ogr2ogr` command that product scripts and the build loader call. No Python
  package links against it: pyogrio ships its own GDAL.
- postgres (latest version)
- `direnv` (see [below](#loading-environment-variables-direnv))

#### Linux (Ubuntu, including WSL)

```bash
sudo apt install gdal-bin postgresql-client direnv
```

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
`python/*` directory, plus `apps/qa`, `apps/dagster`, `apps/notebook-server`,`apps/dcpy_integration_tests`, is its own workspace member with its own `pyproject.toml`); the whole workspace resolves against one committed
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
activates `.venv`, adds `bash/bin` to `PATH`, and sets `BUILD_ENGINE_SCHEMA` from your git branch
name (unset on `main`); each product's `.envrc` calls `source_up` (inheriting the root setup) and
sets product-specific vars like `DATASET` and `BUILD_ENGINE_DB`. With direnv installed and hooked
into your shell, this happens automatically on `cd`.

To hook direnv into your shell, add this to the end of `~/.bashrc` (or `~/.zshrc`, using
`direnv hook zsh`) and open a new terminal:

```bash
eval "$(direnv hook bash)"
```

Then run `direnv allow` in the repo root, and in each product directory the first time you use it.
Changes take effect at your next prompt. After switching branches, run `direnv reload` to update
`BUILD_ENGINE_SCHEMA`.

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

### Upgrading packages

Most `pyproject.toml` dependencies are unpinned, so `uv.lock` is what actually holds versions.
`pyproject.toml` says what's *compatible* (edit it deliberately, with a comment on any pin);
`uv.lock` says what's *installed* (let the upgrade workflow move it).

The "Upgrade uv.lock" workflow (`.github/workflows/python_compile_requirements.yml`) runs
`uv lock --upgrade` every Monday and opens a PR from the `compile_python_reqs` branch. Run it by
hand from the Actions tab to upgrade sooner. The PR description lists what moved, grouped by bump
size (`admin/ops/uv_lock_summary.py`), and flags downgrades and packages split across versions:
both mean the resolver forked onto an old release, and the fix is a floor on `main`. Dependabot only opens Python PRs for security
updates (plus one grouped weekly PR for GitHub Actions) - see `.github/dependabot.yml`.

PRs from `compile_python_reqs` get special handling in `test.yml`: CI rebuilds the Docker images
and runs every product build against them. A PR from any other branch only runs the tests.

If a bump breaks something, pin it in the relevant `pyproject.toml` **on `main`**, not on
`compile_python_reqs` - each weekly run resets that branch from `main`, so a fix pushed only there
is lost if the PR isn't merged first.

Before merging, read the lock diff for major and minor bumps of packages we import directly.

To upgrade locally:

```bash
uv lock --upgrade                      # re-resolve every package to its newest allowed version
uv lock --upgrade-package <name>       # or just one
uv sync --all-packages                 # update your local .venv
```

Avoid `uv sync --upgrade` without `--all-packages`: it relocks the whole workspace but syncs only
the root project, which removes every other workspace member from `.venv`.

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

S3 needs `AWS_S3_ENDPOINT` (e.g. `https://nyc3.digitaloceanspaces.com`), `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. There's no default endpoint: an S3 call without one fails.
`DCPY_S3_ENDPOINT_DENYLIST` (comma-separated hosts) makes dcpy refuse those endpoints outright.

For SQL, prefer `run_sql_command` (on `PATH` via `bash/bin`). Otherwise use `BUILD_ENGINE_SERVER`, a
Postgres connection string of the form `postgresql://{user}:{password}@{host}:{port}`.

## Apps & notebooks

- Run the app stack locally: `cd apps && ./scripts/local-start.sh` (needs env vars from root `.env`).
- Marimo notebooks live in `notebooks/marimo/`, organized by `lifecycle/` and `products/`.

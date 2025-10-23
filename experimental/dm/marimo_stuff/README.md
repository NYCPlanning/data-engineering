> [!IMPORTANT]
> `uv` is the preferred python environment and package manager ([install docs](https://github.com/astral-sh/uv/?tab=readme-ov-file#installation)). While marimo supports all major package managers, it integrates especially tightly with uv ([docs](https://docs.marimo.io/guides/package_management/using_uv/)).

### Virtual environment setup

```bash
# setup
uv venv
uv pip sync requirements.txt
source .venv/bin/activate
uv pip list
```

## Running notebooks

```bash
marimo tutorial --help
marimo tutorial intro
marimo edit my_notebook.py

# via uvx without a virtual environment
uvx marimo tutorial --help
uvx marimo tutorial intro
uvx marimo edit --sandbox my_notebook.py
```

## Updating virtual environment packages

1. (Optional) Edit `requirements.in`
2. Run `uv pip compile --upgrade requirements.in -o requirements.txt`
3. Run `uv pip sync requirements.txt`

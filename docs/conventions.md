# Developer Conventions

## GitHub

### Branches
Single `main` branch. All work happens on feature branches and merges to `main` via pull request.

### Commits
Clean up commits before requesting review — atomic, meaningful commits on `main` are the goal.

```bash
# Rewrite dev branch history before PR review
git rebase -i main        # or: git reset --soft main
git push --force-with-lease
```

Squash or rebase (not a merge commit) when merging a PR.

> [!NOTE]
> See [these tips](https://stackoverflow.com/a/6217314/3984170) to split up a commit on a dev branch.

### Pull requests

- Open a **draft PR early** — helps reviewers track in-progress work and gives the author a checklist.
- PRs **must have a description**: title, links to relevant issues, and a summary of changes.
- For significant or non-obvious code, annotate directly on the diff; for higher-level discussion, use an issue or Teams rather than cluttering the PR.
- Mark **Ready for Review** and assign reviewer(s) when development is complete.
- Re-request review after significant changes post-feedback.

## Code formatting

We use [`ruff`](https://docs.astral.sh/ruff/) for Python and [`sqlfluff`](https://docs.sqlfluff.com/en/stable/index.html) for SQL.

```bash
# Python
uv run ruff check
uv run ruff format

# SQL
sqlfluff lint --dialect postgres --templater jinja products/.../file.sql
sqlfluff fix  --dialect postgres --templater jinja products/.../file.sql
```

For code comments, use consistent tags (inspired by [Better Comments](https://marketplace.visualstudio.com/items?itemName=aaron-bond.better-comments)):

```python
# TODO refactor this function to make it faster
# ! Deprecated function, do not use
# * This is an important note
# ? Should this variable be renamed for consistency?
```

## dbt

Model-layer structure, materialization, indexing, geometry standards, and linting live in
[dbt project conventions](./dbt/project_conventions.md) — read that before adding a model.

We validate conventions with [`dbt-checkpoint`](https://github.com/dbt-checkpoint/dbt-checkpoint) via pre-commit:

```bash
# Environment variables required by the product's profiles.yml must be set
dbt deps --profiles-dir products/product_directory --project-dir products/product_directory
dbt seed --profiles-dir products/product_directory --project-dir products/product_directory
pre-commit run --all-files
```

### Style guides

- [Google Python](https://google.github.io/styleguide/pyguide.html)
- [dbt Labs SQL](https://docs.getdbt.com/best-practices/how-we-style/2-how-we-style-our-sql)
- [GitLab SQL](https://about.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/)
- [dbt best practices](https://docs.getdbt.com/guides/best-practices)
- [Column names as contracts](https://emilyriederer.netlify.app/post/column-name-contracts/)

## Learning resources

- [dbt video series](https://www.youtube.com/playlist?list=PLy4OcwImJzBLJzLYxpxaPUmCWp8j1esvT)
- [dbt Jinja cheat sheet](https://datacoves.com/post/dbt-jinja-functions-cheat-sheet)
- [Boosting PostGIS performance](https://symphony.is/blog/boosting-postgis-performance)
- [Bash: raising errors with `set -e`](http://mywiki.wooledge.org/BashFAQ/105)
- [SQL database connections — GitLab example](https://gitlab.com/gitlab-data/gitlab-data-utils/-/blob/master/gitlabdata/orchestration_utils.py)

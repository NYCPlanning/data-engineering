from pathlib import Path
from dcpy.builds import plan, load

from dbt.cli.main import dbtRunner, dbtRunnerResult

# load.load_source_data("recipe.yml")
recipe = plan.recipe_from_yaml(Path("recipe.lock.yml"))
versions = [ds.version for ds in recipe.inputs.datasets]
versions.sort()
first_version = versions[0]

pairwise_versions = [f'["{versions[i]}", "{versions[i + 1]}"]' for i in range(len(versions) - 1)][:5]
pairwise_versions = f'[{", ".join(pairwise_versions)}]'

dbt = dbtRunner()

# create CLI args as a list of strings
cli_args = ["run", "--vars", f"{{\"first_version\": {first_version}, \"versions\": {pairwise_versions}}}"]

# run the command
res: dbtRunnerResult = dbt.invoke(cli_args)

# inspect the results
for r in res.result:
    print(f"{r.node.name}: {r.status}")

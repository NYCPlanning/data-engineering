# build.plan 
## Command
dcpy.lifecycle.plan.plan
## Inputs
recipe.yaml file, likely with unresolved versions and file types pointing to the ingest datastore.
## Outputs
a fully "planned" recipe.yaml.lock file with versions and files resolved.

# build.load
Loads a database, or pulls data to a local machine, from the ingest datastore
## Command
dcpy.lifecycle.load.load_source_data_from_resolved_recipe

## Inputs
recipe.yaml file, likely with unresolved versions and file types. 
## Outputs
None

# build.build
Executes a data pipeline, usually against a postgres database (increasingly using DBT) but sometimes against a local machine using python and Pandas
## Inputs
A build command
## Outputs
build_outputs path: A path (usually an Azure path) for output files from a build.

# build.package
Takes output files and packages them: namely, documentation (pdfs, etc.) are generated, and small modifications are made to the dataset files.
## Inputs
build_outputs path from the previous step.
## Outputs
package_path: path for the packaged assets

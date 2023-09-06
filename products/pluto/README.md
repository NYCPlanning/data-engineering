# Build Instructions

### I. Build PLUTO Through CI
1. Run the [Pluto Build](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_build.yml) action, specifying the relevant `recipe` file
- Major: `recipe.yml`
Note: You can edit the version attribute of the recipe to set the version. To generate QA against a specific previous version, set a version attribute for the `dcp_pluto` input dataset.

- Minor: `recipe-minor.yml`
Note that the Minor build will mimic the datasets from the latest release, excepting Zoning data, which will use the latest available. 

### II. Build PLUTO on Your Own Machine
In the devcontainer, run steps outlined in the [Pluto Build](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_build.yml)
Start with `Plan Build` then run steps from `Data Setup` through `Apply Corrections`. Take care to note the working directories. 

TODO: we should move the dcpy commands into shell scripts in the project, or orchestrate the entire build from dcpy commands.

## QAQC
Please refer to the [EDM QAQC web application](https://de-qaqc.nycplanningdigital.com/) for cross version comparisons

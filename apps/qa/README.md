# Data Engineering Quality Control and Assurance Application

This web application displays charts and tables to assess the consistency, quality and completeness of a particular build of one of data engineering's data products.

The deployed app is at [edm-data-engineering.nycplanningdigital.com](https://de-qaqc.nycplanningdigital.com/?page=Home)

It's written in Python using the [streamlit](https://streamlit.io/) framework.

## Dev

**To deploy the app, run the github action [Deploy to Dokku - production](https://github.com/NYCPlanning/data-engineering-qaqc/actions/workflows/main.yml).**

> NOTE: This will deploy the app using code in the branch chosen in the "Run workflow" dropdown.

To test changes, run the app locally using the devcontainer (especially via VS Code):

1. From a dev container terminal, navigate to `apps/qa/` and run `python3 -m streamlit run src/index.py`

2. If in VS Code, a popup should appear with an option to navigate to the site in a browser

3. If an error of `Access to localhost was denied` appears in the browser, try navigating to `127.0.0.1:8501` rather than `localhost:8501`

If running GRU qaqc, or working at all on github api functionality, you'll need a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token). The app assumes its stored in the env variable `GHP_TOKEN`.

## Env Variables and Deployment
The deployed app does not have a `.env` file to import environment variables from. If a new environment variable is expected to exist in the the deployed dokku instance, use the following steps ([source](https://tute.io/environment-variables-dokku-config-commands)):

1. In Digital Ocean, navigate to the dokku instance and open a Console (aka terminal)

2. Check the current environment variables using
    ```bash
    dokku config edm-data-engineering
    ```

3. Set the new environment variable using
    ```bash
    dokku config:set edm-data-engineering VAR=Value
    ```

# CEQR

New York City's Environmental Quality Review (CEQR) process requires the use of various tabular, geospatial, and qualitative data resources from city, state, and federal sources. This CEQR data product is a collection of datasets available on the [CEQR Data Hub website](https://nycplanning.github.io/ceqr-data-hub) to help zoning applicants locate the most up to date resources to conduct their environmental reviews.

Each dataset in this CEQR data product falls into one of the following categories:

1. pointing: a link to a webpage
2. pass-through: a dataset that hasn't been transformed
3. pipelined: a dataset that has been transformed

## Development

The DCP SharePoint folder [CEQR Data Hub Final Uploads](https://nyco365.sharepoint.com/sites/NYCPLANNING/cp/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FNYCPLANNING%2Fcp%2FShared%20Documents%2F03%2E%20Process%20Improvement%2F06%2E%20Environmental%20Review%20Reform%2FCEQR%20Methodologies%20Update%2FCEQR%20Hub%2F1%20Data%2FCEQR%20Data%20Hub%20Final%20Uploads) is the source for the Excel workbooks in `/datasets/`.

The distribution point for this data product is the `latest` folder in our Digital Ocean S3 bucket `ceqr-data-hub`.

When there are changes to the seed files in `/seeds/`, the markdown tables in the [CEQR Data Hub repo](https://github.com/NYCPlanning/ceqr-data-hub) must be updated as well using the relevant markdown build files.

## Important files

[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/green_fast_track/recipe.yml)

## Links

[CEQR Data Hub repo](https://github.com/NYCPlanning/ceqr-data-hub)
[CEQR Data Hub website](https://nycplanning.github.io/ceqr-data-hub)

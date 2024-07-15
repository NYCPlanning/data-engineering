# PLUTO

The Primary Land Use Tax Lot Output (PLUTO™) contains extensive land use and geographic data at the tax lot level. PLUTO contain over seventy data fields derived from data files maintained by the Department of City Planning (DCP), Department of Finance (DOF), Department of Citywide Administrative Services (DCAS), and Landmarks Preservation Commission (LPC). DCP has created additional fields based on data obtained from one or more of the major data sources

## Overall file structure

```bash
pluto
├── README.md # this file
├── pluto_build
│   ├── *.sh
│   ├── pluto # with above, bash scripts related to build
│   ├── ae_output_scripts # somewhat exploratory, now defunct scripts for generating custom outputs from an existing build
│   ├── bash # various bash functionality. There are commands here around loading specific source datasets
│   ├── data # raw csv inputs, such as corrections
│   ├── python # two python scripts relating to wrangling source data
│   ├── sql # sql files used in build
│   └── templates # library templates for source data with custom logic in pluto folder
├── recipe.yml # major recipe file, defines inputs to PLUTO
├── recipe-minor.yml # minor recipe, defines inputs to PLUTO holding most source data versions constant from previous build
├── schemas
```

## Important files

[metadata](https://github.com/NYCPlanning/product-metadata/blob/main/products/pluto/pluto/metadata.yml)
[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/pluto/recipe.yml)
[recipe-minor](https://github.com/NYCPlanning/data-engineering/blob/main/products/pluto/recipe-minor.yml) - holds most source data versions constant from last published build
[Github workflow](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/pluto_build.yml) - config file used to build PLUTO via Github Actions
[Github workflow - PTS input data](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/pluto_input_pts.yml) - config file used to ingest PTS via data library. See template in `pluto_build/templates` folder as well.
[Github workflow - PTS input data](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/pluto_input_cama.yml) - config file used to ingest CAMA via data library. See template in `pluto_build/templates` folder as well.
[Github workflow - PTS input data](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/pluto_input_numbldgs.yml) - config file used to ingest numbldgs data via data library. See template in `pluto_build/templates` folder as well.

## Links

[PLUTO on Bytes](https://home.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page)
[PLUTO on OpenData](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks/about_data)

For more in-depth information on PLUTO and its schema, check out its [wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-PLUTO)

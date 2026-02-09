# Template DB

Template DB is a mock data product and is meant to be:

- a model of our latest standard approaches to building data products
- an end-to-end test of our data platform
- a low-risk data pipeline to experiment with

## Latest output files

| Type                       | Shapefile                  | FileGDB | CSV                  |
| -------------------------- | -------------------------- | ------- | -------------------- |
| Clipped                    | [dataset_name]()           | NA      |
| Unclipped (Water Included) | [dataset_name_unclipped]() | NA      |
| No Geometry                | NA                         | NA      | [dataset_name.csv]() |

## Additional files

- [Source Data versions]()
- [Related export]()
- [Related export]()

---

## Output files

- File Name A
- File Name B

> [notes or table about output files]

## Source data

- dpr_parksproperties
- dcp_yyy
- dof_xxx

> [notes or table about source data]

## Data Dictionary

### File Name A

#### column_a

- **Display Name**: `Column A`
- **Description**: Description of the values in this column and their meaning.

#### column_b

- **Display Name**: `Column B`
- **Description**: Description of the values in this column and their meaning.

### File Name B

#### column_a

- **Display Name**: `Column A`
- **Description**: Description of the values in this column and their meaning.

## QAQC

Please refer to the [EDM QAQC web application](https://edm-data-engineering.nycplanningdigital.com) for cross version comparisons

---

## Build process

## Build logic

1. Load input datasets from `edm-recipes` DigitalOcean bucket to a Postgres database

2. ...

3. Publish outputs to `edm-publishing` DigitalOcean bucket in a versioned directory and, if appropriate, the `latest` directory

> [notes about build logic]

## Approach Details

[Explain unique/non-trivial methods]

[Details about the purpose of this dataset and why any noteworthy decisions were made]

[Details about alternative approaches and why they weren't chosen]

---

## Dev notes

> This fictional data product is meant to be
>
> - an end-to-end test of our data platform
> - an example of the approaches we'd like to use in all data products

...

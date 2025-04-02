# Distribute

## Terms
- `Dataset`: Abstractly, a single table of data and metadata about it.
- `Product`: A suite of datasets. E.g. LION, which contains multiple `datasets`, such as "2010 Census Blocks" or "Community Districts (water areas Included)"
- `Dataset File`: Actual file export for a dataset, e.g. a csv, or a shapefile. Note: there may be slight variations between `dataset files` for the same `dataset`. e.g. columns in the shapefile for PLUTO will have slightly different columns than the csv.
- `Attachment`: README's, data dictionaries, etc.
- `Dataset Package`: Instance of a `Dataset`, meaning metadata, attachments, and dataset files.
- `Product Package`: Versioned collection of Dataset Packages.

## How to distribute to Socrata
Socrata datasets consist of, effectively, one table. In the past, a single Socrata dataset could contain multiple shapefile layers, but that's no longer the case. So for us:

One `Dataset Package` -> one Socrata page.

So the first step is to assemble the `Dataset Package`.

#### Dataset Package Structure
The package must be structured like so:
- `/attachments/`: folder containing all attachments (except metadata.yml)
- `/dataset_files/`: folder containing data files for the dataset, e.g. the shapefile, the csv, etc.
- `metadata.yml`: describes all columns, destinations, expected attachments, and expected dataset files. It also drives all the package-level validation.

#### Pushing from S3 via CLI:

To push from S3, the Dataset Package must exist in `edm-publishing` in the `product_datasets` folder, under it's relevant product folder. The directory structure is:
```
edm-publishing / product_dataset / {product_name} / package / {version} / {dataset_name} / [package files here]
```
Note: In many cases the product has only one dataset, e.g. Facilities. In that case, the convention is to just use the same product_name and dataset_name. E.g
```
edm-publishing / product_dataset / facilities / package / 24v2 / facilities / [package files here]
```
To actually push:
```
python -m dcpy lifecycle distribute socrata from_s3 [args]
```
The --help flag will list required args.

Note that unless you explicitly specify `--publish`, only a draft will be created on Socrata, and you'll need to manually apply it via the socrata GUI. The output of the command will tell you the revision number, which you can find via search on Socrata. There you can review data changes, metadata changes, etc, before actually publishing.

##### Quick Tip
If package validation fails, you can validate locally by running the validation commands locally:
```
python -m dcpy lifecycle package validate [your package path here]
```

#### Pushing from S3 via Github Action:
[Relevant Action](https://github.com/NYCPlanning/data-engineering/actions/workflows/socrata_publish_dataset.yml). The same options as above apply. You'll need to supply the product name, the dataset name, and the version.

Note: this is a low-risk operation when you don't tick the box to publish the dataset.


#### Assembling and Pushing from Bytes (local flow)
1. Ensure you have the product-metadata repo cloned locally, and have set the `PRODUCT_METADATA_REPO_PATH` var
2. Run the lifecycle package and dist command in scripts. This will allow you to target specific destinations for a given dataset. For each dataset specified, this command will pull relevant files from `bytes`, generate the OTI XLSX when specified in the files, then distribute the data to Socrata. This command can filter by dataset name, and destination tag. (note: the conditions are combined with AND logic)

For Example
``` sh
python -m dcpy lifecycle scripts package_and_dist from_bytes_to_socrata \
  lion \
  24d \
  -y \
  -e socrata \
  -d atomic_polygons \
  -d other_dataset \
  -t socrata_unpublished
```
This will package and distribute to all socrata destinations for the `lion` product, version 24d (-y will skip dataset file validation) where the dataset is either `atomic_polygons` or `other_dataset`, where the destination is tagged as `socrata_unpublished`.


## The Socrata Publish Flow
In our publishing connector, the flow for distributing is as follows:

1. create a new revision for the dataset, and discard other open revisions.
2. upload attachments, and update dataset-level metadata. (e.g. the dataset description or tags)
3. Upload the dataset itself. Currently only shapefiles are supported.
4. _Attempt_ to update column metadata for the uploaded dataset. This step is placed last because it's the most finicky at the moment, as it entails reconciling our uploaded columns, Socrata's existing columns, and our metadata. However, should this step fail, you can still go manually apply the revision in the Socrata GUI.

## Applying Revisions in Socrata

In the case of errors, or if the the `--publish` flag isn't supplied, you'll have to manually apply on the Socrata website. Below is an example push without a publish:

```
INFO:dcpy:Pushing shapefiles at .package/product_datasets/template_db/package/20231227/template_db/dataset_files/templatedb_points.shp.zip to b7pm-uzu7 - rev: 32
INFO:dcpy:Updating Columns at https://data.cityofnewyork.us/api/publishing/v1/source/202450926/schema/199848287
INFO:dcpy:                    Columns from dataset page: {'place_name', 'bbl', 'place_type', 'wkb_geometry', 'borough'}
INFO:dcpy:                    Columns from our metadata: ['place_name', 'bbl', 'place_type', 'borough', 'wkb_geometry']
INFO:dcpy:
INFO:dcpy:Finished syncing product to Socrata, but did not publish. Find revision 32, and apply manually
INFO:dcpy:            here https://data.cityofnewyork.us/d/b7pm-uzu7/revisions/32
```

Follow the provided link. Here you can review the modified data and metadata. Hit `Update` in the top right to apply the revision.
![template_db_socrata](https://github.com/NYCPlanning/data-engineering/assets/11164730/b0c24251-00e3-4be1-99a6-6cf015240cc6)

Before publishing,
- check the row count
- review the "Metadata Changes" (hit the Details dropdown). Make sure that everything looks fine. (e.g. you haven't removed fields, or completely removed an attachment, etc.)

## Generating Metadata

So you need to generate a `metadata.yml` file. There are a few options that will each get you part of the way:

#### Socrata Connector
``` sh
python -m dcpy connectors socrata metadata export {four-four here}
```

The limitations here are when you're working from an old datasource. It will correctly pull certain dataset-level fields (e.g. Description), but unfortonutely no column metadata. (yet)

#### ESRI FeatureServer Connector
``` sh
python -m dcpy connectors esri metadata export {feature-server here}

-- e.g.

python -m dcpy connectors esri metadata export https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Borough_Boundary/FeatureServer/0
```

#### ESRI PDF parser

The last resort for grabbing column metadata. This is by far the most fragile of the methods.
1. Open the ESRI pdf in the **Mac Preview app** (Important - copying from other apps doesn't work)
2. Copy paste the document contents into a text file
3. Run:

``` sh
python -m dcpy lifecycle package esri parse_pdf_text {path to your text file}
```


#### OLD STYLE DATASETS

These are datasets created before 201x (maybe 2016'ish?) and unfortunately we need to distribute them manually.

In this case, make sure you're logged in, and then:
1) navigate to the dataset itself
2) Update the dataset itself: Click "Edit" in the top right corner, and follow steps to upload the new dataset
3) Update the metadata: Click "About" in the top-right, and click "Edit Metadata"
    - Scroll all the way down
    - Delete the old attachment(s)
    - Upload the new attachemt(s)




#### POTENTIAL ISSUES

##### I've pushed to socrata, but when I visit the revision page, the `Update` button is greyed out.
Hover over the `Update` button, and it should point you towards the cause. Usually it's a metadata problem.

*If it's a metadata problem, but nothing seems wrong (ie nothing is bright red in the metadata modal)* Usually you can fix this by adding a space and removing it, or some similar non-change. Hit `Save`, and likely you'll be able to update.

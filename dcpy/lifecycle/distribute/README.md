# Distribute

## Terms
- `Product`: a suite of datasets. E.g. LION, which contains multiple `datasets`, such as "2010 Census Blocks" or "Community Districts (water areas Included)"
- `Dataset` : roughly analogous to what you'd find on a Socrata page. Effectively it's metadata, attachments, and dataset files. 
- `Dataset File`: actual file for a dataset, e.g. a csv, or a shapefile. Note: there may be slight variations between `dataset files` for the same `dataset`. e.g. columns in the shapefile for PLUTO will have slightly different columns than the csv. 
- `Attachment`: README's, data dictionaries, etc.

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
python -m dcpy.cli lifecycle distribute socrata from_s3 [args]
```
The --help flag will list required args.

Note that unless you explicitly specify `--publish`, only a draft will be created on Socrata, and you'll need to manually apply it via the socrata GUI. The output of the command will tell you the revision number, which you can find via search on Socrata. There you can review data changes, metadata changes, etc, before actually publishing. 

##### Quick Tip
If package validation fails, you can validate locally by running the validation commands locally:
```
python -m dcpy.cli lifecycle package validate [your package path here]
```

#### Pushing from S3 via Github Action:
[Relevant Action](https://github.com/NYCPlanning/data-engineering/actions/workflows/socrata_publish_dataset.yml). The same options as above apply. You'll need to supply the product name, the dataset name, and the version.

Note: this is a low-risk operation when you don't tick the box to publish the dataset. 

## The Socrata Publish Flow
In our publishing connector, the flow for distributing is as follows: 

- create a new revision for the dataset, and discard other open revisions. 
- upload attachments, and update dataset-level metadata. (e.g. the dataset description or tags) 
- Upload the dataset itself. Currently only shapefiles are supported. 
- _Attempt_ to update column metadata for the uploaded dataset. This step is placed last because it's the most finicky at the moment, as it entails reconciling our uploaded columns, Socrata's existing columns, and our metadata. However, should this step fail, you can still go manually apply the revision in the Socrata GUI. 

## Applying Revisions in Socrata

In the case of errors, or if the the `--publish` flag isn't supplied, you'll have to manually apply on the Socrata website. Below is an example push without a publish:

```
INFO:dcpy:Pushing shapefiles at .package/product_datasets/template_db/package/20231227/template_db/dataset_files/templatedb_points.shp.zip to b7pm-uzu7 - rev: 32
INFO:dcpy:Updating Columns at https://data.cityofnewyork.us/api/publishing/v1/source/202450926/schema/199848287
INFO:dcpy:                    Columns in uploaded data: {'place_name', 'bbl', 'place_type', 'wkb_geometry', 'borough'}
INFO:dcpy:                    Columns from our metadata: ['place_name', 'bbl', 'place_type', 'borough', 'wkb_geometry']
INFO:dcpy:
INFO:dcpy:Finished syncing product to Socrata, but did not publish. Find revision 32, and apply manually
INFO:dcpy:            here https://data.cityofnewyork.us/d/b7pm-uzu7/revisions/32
```

[ TODO: Image Here ]
Follow the provided link. Here you can review the modified data and metadata. Hit `Update` in the top right to apply the revision. 


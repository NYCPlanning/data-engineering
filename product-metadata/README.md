# product-metadata

Dataset and product specifications for DCP data products. Each product folder under `products/`
contains one or more `metadata.yml` files describing columns, destinations (Socrata open data,
etc.), and distribution configuration. Shared defaults live in `snippets/` and `packaging/`.

This directory was migrated from the standalone `NYCPlanning/product-metadata` repository in
[issue #2436](https://github.com/NYCPlanning/data-engineering/issues/2436). That repo is now
deprecated and archived.

`PRODUCT_METADATA_REPO_PATH` (read by `dcpy`) defaults to this directory; set it only to point
at a different checkout.

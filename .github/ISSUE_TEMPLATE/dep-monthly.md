---
name: Monthly Run DEP CATS permits
about: Scheduled DEP CATS permits build succeeded.
title: "`dep_cats_permits` ready for QA"
labels: 'data update'
---

A scheduled run of `dep_cats_permits` is complete! 🎉

## Staging files in Digital Ocean

- [version.txt](https://nyc3.digitaloceanspaces.com/edm-publishing/datasets/dep_cats_permits/staging/version.txt)
- [dep_cats_permits.zip](https://nyc3.digitaloceanspaces.com/edm-publishing/datasets/dep_cats_permits/staging/dep_cats_permits.zip)
- [dep_cats_permits.csv](https://nyc3.digitaloceanspaces.com/edm-publishing/datasets/dep_cats_permits/staging/dep_cats_permits.csv)
- [ReadMe_DEPCATS.pdf](https://nyc3.digitaloceanspaces.com/edm-publishing/datasets/dep_cats_permits/staging/ReadMe_DEPCATS.pdf)

## Next Steps

The ITD QA team will be notified in the `edm-data-operations` repo [here](https://github.com/NYCPlanning/edm-data-operations) of a difference between the staging and production folders.

They will use a github action in that repo to publish staged data which will move staging files to production.

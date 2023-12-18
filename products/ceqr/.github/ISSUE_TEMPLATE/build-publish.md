---
name: Publish Dataset
about: Runs recipe to build specified recipe
title: '[publish] {{ env.DATASET }}'
labels: 'data update'
assignees:
    - ileoyu
    - OmarOrtiz1
    - jpiacentinidcp
    - rtblair
---

A fresh run of `{{ env.DATASET }}` is complete! 🎉

This issue facilitates the review and publishing of data from the staging to the production location.

## Staging files output

- [ ] [version.txt](https://nyc3.digitaloceanspaces.com/edm-publishing/ceqr-app-data-staging/{{ env.DATASET }}/latest/version.txt)
- [ ] [{{ env.DATASET }}.zip](https://nyc3.digitaloceanspaces.com/edm-publishing/ceqr-app-data-staging/{{ env.DATASET }}/latest/{{ env.DATASET }}.zip)
- [ ] [{{ env.DATASET }}.csv](https://nyc3.digitaloceanspaces.com/edm-publishing/ceqr-app-data-staging/{{ env.DATASET }}/latest/{{ env.DATASET }}.csv)

## Next Steps

If you have manually checked above files and they seem to be ok, comment `[publish]` under this issue.
This would allow github actions to move staging files to production.
Feel free to close this issue once it's all complete. Thanks!

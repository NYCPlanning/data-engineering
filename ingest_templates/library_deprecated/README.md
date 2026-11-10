# Deprecated library templates

Library templates retired without an ingest replacement. They are kept rather than deleted so the
source definition is still findable if one turns out to be wanted.

Nothing reads this folder. `glob("*.yml")` on `ingest_templates/` is not recursive, so these are
invisible to ingest, and moving a template here removes it from `dcpy/library/templates/`, so
library can no longer archive it either.

Three reasons a template ended up here:

- **Superseded by a rename.** `dob_jobapplications` and `dob_permitissuance` were migrated as
  `dob_bis_applications` and `dob_bis_permits`.
- **No consumer.** No product recipe, build script, or query references the dataset. Some are
  referenced only from workflows under `.github/workflows/archive/`, or from lines that are
  commented out. `council_members` had an ingest template too, and its Socrata source has since
  become a different dataset (every council member since 1999).
- **Archived by hand instead.** `dcp_pop_acs`, `dcp_pop_decennial_dhc`, and
  `dcp_pop_decennial_ddhca` are still read by factfinder, as whole Excel workbooks. Library only
  copied them as-is, so a new delivery is uploaded straight to `edm-recipes`. See
  `products/factfinder/README.md`.

Before reviving one, check whether its source still exists. Several point at
`s3://edm-recipes/tmp/`, which has been cleared.

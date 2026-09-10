# Deprecated library templates

Library templates for datasets nothing loads any more. They are kept rather than deleted so the
source definition is still findable if one turns out to be wanted.

Nothing reads this folder. `glob("*.yml")` on `ingest_templates/` is not recursive, so these are
invisible to ingest, and moving a template here removes it from `dcpy/library/templates/`, so
library can no longer archive it either.

Two reasons a template ended up here:

- **Superseded by a rename.** `dob_jobapplications` and `dob_permitissuance` were migrated as
  `dob_bis_applications` and `dob_bis_permits`.
- **No consumer.** No product recipe, build script, or query references the dataset. Some are
  referenced only from workflows under `.github/workflows/archive/`, or from lines that are
  commented out.

Before reviving one, check whether its source still exists. Several point at
`s3://edm-recipes/tmp/`, which has been cleared.

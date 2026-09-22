{#
Fails (returns the missing key(s)) if seeds/config.csv is missing a row for any key a
model actually reads via the config_value() macro - the idiomatic dbt replacement for
the ad-hoc run_query/raise_compiler_error guard int__ldf_header.sql used to run inline
at parse time (removed 2026-09-22; see git history). not_null/unique on config's own
key/value columns (seeds.yml) catch a blank or duplicated row; this catches a row
missing outright, which a scalar-subquery read (see config_value.sql) can't tell apart
from "key present but null" on its own.

Keep required_keys in sync with config_value() call sites - `grep -rn "config_value("
models/` finds them all.
#}

{% set required_keys = [
    "ldf_previous_version",
    "ldf_old_release_date",
    "ldf_new_release",
    "ldf_new_release_date",
    "thined_file_tag",
    "thined_version",
] %}

WITH required_keys (key) AS (
    VALUES {{ "('" ~ required_keys | join("'), ('") ~ "')" }}
)

SELECT required_keys.key AS missing_config_key
FROM required_keys
LEFT JOIN {{ ref('config') }} AS config ON required_keys.key = config.key
WHERE config.key IS NULL

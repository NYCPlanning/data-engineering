# LIFT maps

Configuration for the LIFT CARTO Builder maps, managed with the [CARTO CLI](https://docs.carto.com/carto-cli) (`carto`).

| File | Map | Data |
|---|---|---|
| | [LIFT Tracker Map](https://clausa.app.carto.com/builder/73a850dd-0c8d-4eef-ad31-d32daf5c8160) | `shared.lift_26v1_lots` |
| [`lift_tracker_dev.json`](./lift_tracker_dev.json) | [LIFT Tracker Map - DEV 1](https://clausa.app.carto.com/builder/8735d5c1-94eb-472f-a2b8-fb8106c54e9b) | `shared.lift_dev_lots`, `shared.lift_dev_community_districts`, `shared.lift_dev_council_districts` |
| [`lift_tracker_dev_2.json`](./lift_tracker_dev_2.json) | [LIFT Tracker Map - DEV 2](https://clausa.app.carto.com/builder/efd89914-af02-47b1-b772-9505946fe34d) | `shared.lift_dev_lots`, `shared.lift_dev_2_community_districts`, `shared.lift_dev_council_districts` |

LIFT Tracker Map was the first map, made to show that a password-protected map of this data works.
It isn't managed here.
DEV 1 is the map reviewers gave feedback on ([#2739](https://github.com/NYCPlanning/data-engineering/issues/2739)).
DEV 2 started as a copy of DEV 1, and feedback changes are tried there first so DEV 1 stays as reviewed.
Which maps to keep is still open.
A map's title can be changed, and one map's config can be pushed to another, so consolidating later is cheap.

DEV 1 and DEV 2 read the same lots and council district tables, so a new upload of either changes both.
DEV 2 has its own community districts table, since it groups districts differently from DEV 1.
Check both maps' queries still run after an upload that adds or renames columns.

The JSON is a snapshot of what `carto maps get` returns, minus fields the server adds on read and the map's password (`publicMapPassword`).
Builder saves to the same map, so the file goes stale whenever someone edits in Builder.
Pull before editing it, and commit it after changing the map.

## Map design

The decisions behind DEV 2, so a similar map can be built from new data without repeating the trial and error.
[`lift_tracker_dev_2.json`](./lift_tracker_dev_2.json) has the exact settings.

### Data

Shape the table in the build (`lift_supplemented_map`), not in each map dataset's SQL.
SQL in the map has to be repeated in every dataset that reads the table, and other maps of the same table don't get it.

- Map widgets, the table, pop-ups, and label layers show `<column>_label` columns, not the raw columns. A label is text, never null, and says why a value is missing, like "Not a LIFT site", or "Not set" for a LIFT site missing a value.
  A raw null shows up as a blank bar or the word "null", and a raw boolean as true/false, and viewers can't tell what either means.
  `tests/assert_map_labels_not_null.sql` checks every `_label` column.
- Keep the raw columns raw, with their nulls and types, so the file is still easy to query outside the map.
- Add a `_label` column only for columns the map shows.
- Only label a missing value as "no" when the answer is known. `ahft_label` calls joint interest areas "Non-AHFT district", since AHFT doesn't rank them, but lots with no community district get "No community district".
- Keep numbers numeric so they can drive sliders.

### Layers

- The lots are two layers on the same table: points (`ST_CENTROID`) below zoom 11 and lot shapes from zoom 11. Both are colored by `is_lift_site_label`.
- The points query sorts LIFT sites last (`ORDER BY is_lift_site`), so they're drawn on top of other lots instead of hidden under them.
- Boundary layers start off. Clicking a community district filters to it ("Filter by feature" set to all layers, on the layer's Click tab).
  A layer needs a fill for clicks to land on it, so the council layer has a near-white one. Both boundary layers have hover pop-ups off.

### Colors

Colors come from the reviewers' design standards.

| Use | Color |
|---|---|
| LIFT site points and shapes | `#D9620C` |
| Other lot points | `#B1C0EC`, a quarter of the way from `#CAD7FA` to `#667DC3` |
| Other lot shapes | `#CAD7FA` |
| AHFT district fill | `#667DC3` at 0.02 opacity |
| Non-AHFT district and council district fill | `#F6F8FE` |
| Outlines: lot shapes and districts | `#120062` |

- Points need a stronger color than area fills. At `#CAD7FA`, small points almost disappear on the light basemap. At `#667DC3`, the many blue points bury the orange ones. `#B1C0EC` sits between, keeping LIFT points the stronger color against the basemap.
- Points have no outline. A data-driven outline color (like a dark rim on LIFT points only) adds a second legend section for the layer, and the legend can't hide it.
- Lot shapes are at 0.35 opacity so basemap buildings show through. The outline keeps lot edges visible.
- District fills are faint on purpose. The district outlines show the boundaries, and the fill mostly gives clicks something to land on.
- Check contrast against the basemap, not only between categories. Two colors can be easy to tell apart and still both fade into the basemap.

### Filters

- Every filter is a widget, so filters and summary numbers sit together in one panel. Category widgets have search and multi-select, show counts, and filter each other. Selecting "AHFT district" narrows the community district widget to the AHFT districts.
- Lot area is a range widget over the data's full extent, up to 214M sq ft (JFK Airport). The slider is too coarse at that scale, so type numbers in its boxes. Setting the widget's max to 100,000 dropped the lots above it.
- A widget can only show the values picked, not hide them. The DCAS flags widget's bars are combinations of flags, like "Underwater, Waterfront", so it shows which flags lots carry but can't hide flagged lots. DEV 1 hides them with a SQL parameter in the bar above the map ("Hide lots flagged as"), for comparison.
- A SQL parameter changes every lot query, so it has to be added to both the points and the lot shapes datasets. Write its SQL so its default filters nothing, including rows with no value.
- In the description, put how-to-use text under a "Map notes" heading and data caveats under "Data notes".

### Widgets and table

- Summary and filter widgets count the whole dataset (`global: true`). Recounting for the map view costs more than it's worth, and adds little when zoomed in on a few lots.
- The table lists only lots in the map view (`global: false`), so nearby lots can be compared. Search it by address or BBL by picking the column. Lots without geometry never show up in it.
- Table search starts on the first column, so Address comes first.
- The address search bar at the top of the map is on too, but it searches everywhere, not only NYC, so an address without a ZIP code can land in another state. The map description says to add the ZIP code.
- The table can't be given a default sort. It has no sort setting, and an `ORDER BY` in the dataset's query doesn't change its row order.
- Keep the panel short: the summary numbers first, then the requested filters. Summary widgets and the table start open, filters start collapsed.
- The LIFT site filter comes right after the summary numbers. Its bars show the LIFT site count, and clicking one filters to LIFT sites, so it does the job of a separate count widget.
- Address and BBL filters come last, before the table. They weren't requested, so they're there for reviewers to try.
- Counts use the `DECIMAL_ZERO` format, which adds thousands separators.
- The panel floats, so it sizes to the open widgets and can be hidden.

### Text

- Add a widget note only where its meaning isn't obvious, like what a blank bar means. Leave counts out, since they change with the data.
- Explain how to use the map in its description. Notes on the table widget don't render.
- Click pop-ups show the same fields for every lot, so LIFT-only fields are blank on other lots.

## Load the data

| Table | From |
|---|---|
| `shared.lift_dev_lots` | the build's `lift_supplemented_map` |
| `shared.lift_dev_community_districts` | an older build of `community_districts_map`, with three AHFT groups. Leave it as is while DEV 1 stays as reviewed. |
| `shared.lift_dev_2_community_districts` | the build's `community_districts_map` |
| `shared.lift_dev_council_districts` | `dcp_councildistricts` in `edm-recipes` (`coundist` and geometry), not a LIFT input |

Upload GeoParquet rather than `lift.gdb.zip`: FileGDB has no boolean type, so flags would arrive in CARTO as 0/1 integers.

```bash
# from a build's DuckDB file; <schema> is the build's BUILD_ENGINE_SCHEMA
python -c "
import duckdb
c = duckdb.connect('lift_26v1.duckdb', read_only=True)
c.sql('load spatial')
c.sql(\"copy (select * from <schema>.lift_supplemented_map) to 'lift_dev_lots.parquet' (format parquet)\")
c.sql(\"copy (select * from <schema>.community_districts_map) to 'lift_dev_2_community_districts.parquet' (format parquet)\")
"
carto import --file lift_dev_lots.parquet --connection carto_dw \
  --destination carto-dw-ac-xh9k79q8.shared.lift_dev_lots --overwrite
carto import --file lift_dev_2_community_districts.parquet --connection carto_dw \
  --destination carto-dw-ac-xh9k79q8.shared.lift_dev_2_community_districts --overwrite
```

The 45 lots without PLUTO geometry are imported without a location. They're counted by widgets but not drawn.

If the upload adds columns, change the wording of each dataset's query (not its result) before using the new columns, e.g. alias the table or qualify a column.
Builder caches each dataset's columns by query text, so re-sending the same query isn't enough.
Otherwise Builder drops popup fields that reference the new columns the next time anyone saves in Builder.

## Pull and push the config

Shown for DEV 1. For DEV 2, use its map id and `lift_tracker_dev_2.json`.

```bash
# pull
carto maps get 8735d5c1-94eb-472f-a2b8-fb8106c54e9b --json \
  | jq -S 'del(.publicMapPassword) | .datasets |= map(del(.mapId, .connectionName, .connectionViewerCredentials,
      .connectionViewerCredentialsAttached, .providerId, .sourceWorkflowId,
      .sourceWorkflowNodeId, .createdAt, .updatedAt))' \
  > lift_tracker_dev.json

# check, then push
carto maps validate lift_tracker_dev.json
carto maps update 8735d5c1-94eb-472f-a2b8-fb8106c54e9b lift_tracker_dev.json
```

`keplerMapConfig` (layers, widgets, popups, filters) is replaced wholesale on update, so always push a full file, never a partial one.
A push changes the saved map but not what viewers see. See [Publish](#publish).

## Publish

Anyone opening a map's link sees its last published snapshot, not the saved map.
Builder saves and CLI pushes only change the saved map, so publish after either one when viewers should see the change.
Publishing freezes the saved starting state too, so pull first and check it against the save checklist under [Gotchas](#gotchas).

```bash
carto maps publish efd89914-af02-47b1-b772-9505946fe34d

# or push and publish in one step
carto maps update efd89914-af02-47b1-b772-9505946fe34d lift_tracker_dev_2.json --publish
```

## Create a new map from the config

A new map can't reuse the DEV map's dataset ids, so convert the file first.
[`to_create_bundle.py`](./to_create_bundle.py) swaps them for `$ref` names.
The new map starts private.

```bash
python to_create_bundle.py lift_tracker_dev.json "New map title" > new_map.json
carto maps create new_map.json
```

The CLI can't put the new map in a project: `containerId` is ignored on create and update, and `carto projects add` only adds a shortcut.
Move it into the project in CARTO's UI.

## Gotchas

- **Add layers for new table sources in Builder, then style them from the CLI.**
  Two layers created from the CLI on new table-type datasets validated and rendered, but Builder dropped them on load and on its next save.
  The same styling applied to layers Builder created survived.
  Builder writes `_carto_feature_id` into a table dataset's `columns`, and the dropped layers' datasets didn't have it, so that's the likely cause, but it isn't confirmed.
- **Reload Builder after every CLI write, before clicking anything.**
  An open Builder tab doesn't see outside changes, and its next save overwrites them with its own older copy.
- **The "Hide lots flagged as" filter defaults to "(none)".**
  Builder won't save an empty selection, and the CLI turns an empty default into "all selected", which would hide every flagged lot.
  "(none)" matches no flag, so it hides nothing.
  Builder saves have stored every flag as selected, in the control (`sqlParameters`) and in the lot datasets' `queryParameters`. Pull after a Builder save and reset both to "(none)".
- **A Builder save stores what the tab is showing as everyone's starting state.** Before saving, check:
  - the view is zoomed out to the whole city
  - in DEV 1, every top-bar control is at its default: all boroughs, all community districts, "All districts", and "(none)" for flags
  - no widget filters are selected and no area selection is drawn
  - the two district boundary layers are off and the layer list is collapsed
  - only the two count widgets are expanded, plus the table in DEV 2
  - the widget panel is open, not hidden
  - in DEV 2, the widgets are in their committed order, since a stale tab can save an older order
- **`agent` is stripped on update** while CARTO AI is disabled for the organization. The CLI warns about it, and the map's setting is left as is.

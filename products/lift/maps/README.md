# LIFT maps

Configuration for the LIFT CARTO Builder maps, managed with the [CARTO CLI](https://docs.carto.com/carto-cli) (`carto`).

| File | Map | Data |
|---|---|---|
| [`lift_tracker_dev.json`](./lift_tracker_dev.json) | [LIFT Tracker Map - DEV](https://clausa.app.carto.com/builder/8735d5c1-94eb-472f-a2b8-fb8106c54e9b) | `shared.lift_dev_lots`, `shared.lift_dev_community_districts`, `shared.lift_dev_council_districts` |

The DEV map is a copy of the map under review, used to try changes without reviewers seeing them.
The reviewed map and its table (`shared.lift_26v1_lots`) aren't managed here.

The JSON is a snapshot of what `carto maps get` returns, minus fields the server adds on read.
Builder saves to the same map, so the file goes stale whenever someone edits in Builder.
Pull before editing it, and commit it after changing the map.

## Load the data

| Table | From |
|---|---|
| `shared.lift_dev_lots` | the build's `lift_supplemented_map` |
| `shared.lift_dev_community_districts` | the build's `community_districts_map` |
| `shared.lift_dev_council_districts` | `dcp_councildistricts` in `edm-recipes` (`coundist` and geometry), not a LIFT input |

Upload GeoParquet rather than `lift.gdb.zip`: FileGDB has no boolean type, so flags would arrive in CARTO as 0/1 integers.

```bash
# from a build's DuckDB file; <schema> is the build's BUILD_ENGINE_SCHEMA
python -c "
import duckdb
c = duckdb.connect('lift_26v1.duckdb', read_only=True)
c.sql('load spatial')
c.sql(\"copy (select * from <schema>.lift_supplemented_map) to 'lift_dev_lots.parquet' (format parquet)\")
c.sql(\"copy (select * from <schema>.community_districts_map) to 'lift_dev_community_districts.parquet' (format parquet)\")
"
carto import --file lift_dev_lots.parquet --connection carto_dw \
  --destination carto-dw-ac-xh9k79q8.shared.lift_dev_lots --overwrite
carto import --file lift_dev_community_districts.parquet --connection carto_dw \
  --destination carto-dw-ac-xh9k79q8.shared.lift_dev_community_districts --overwrite
```

The 45 lots without PLUTO geometry are imported without a location. They're counted by widgets but not drawn.

If the upload adds columns, change the wording of each dataset's query (not its result) before using the new columns, e.g. alias the table or qualify a column.
Builder caches each dataset's columns by query text, so re-sending the same query isn't enough.
Otherwise Builder drops popup fields that reference the new columns the next time anyone saves in Builder.

## Pull and push the config

```bash
# pull
carto maps get 8735d5c1-94eb-472f-a2b8-fb8106c54e9b --json \
  | jq -S '.datasets |= map(del(.mapId, .connectionName, .connectionViewerCredentials,
      .connectionViewerCredentialsAttached, .providerId, .sourceWorkflowId,
      .sourceWorkflowNodeId, .createdAt, .updatedAt))' \
  > lift_tracker_dev.json

# check, then push
carto maps validate lift_tracker_dev.json
carto maps update 8735d5c1-94eb-472f-a2b8-fb8106c54e9b lift_tracker_dev.json
```

`keplerMapConfig` (layers, widgets, popups, filters) is replaced wholesale on update, so always push a full file, never a partial one.
Nothing is published until `carto maps publish <map-id>`.

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
- **A Builder save stores what the tab is showing as everyone's starting state.** Before saving, check:
  - the view is zoomed out to the whole city
  - every top-bar control is at its default: all boroughs, all community districts, "All districts", and "(none)" for flags
  - the two district boundary layers are off
  - only the two count widgets are expanded
- **`agent` is stripped on update** while CARTO AI is disabled for the organization. The CLI warns about it, and the map's setting is left as is.

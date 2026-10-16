# Level of service and capital spending

Draft notes on linking capital spending to the levels of service it's meant to change.
The goal is to set levels of service as goals and use them to steer the capital budget.
The services and attributes below are our reading of names, not checked against data, and no model uses them.

## How the pieces connect

A level of service is a measure of an asset, like a park's acres or a street's pavement condition.
It comes from asset inventories, not from capital data.
Capital data can at best say what a project or category is expected to change.

```
level of service goal (a target on a condition attribute, maybe by geography)
  └ condition attribute (per asset type, measured from an asset inventory)
      └ ten-year plan category (expected to change these attributes)
          └ budget line and planned commitment (dollars)
```

Budgets are set and moved by budget line and category, so the category is the grain for steering.
Checking whether a level of service actually changed needs the asset inventory, by site or segment.

A category's lifecycle (State of Good Repair, Programmatic Replacement, Program Expansion) is the intended change to the asset base: maintain, replace, or expand.
It isn't a change in service.
A street reconstruction is State of Good Repair, but it can also widen sidewalks, which changes pedestrian space without adding an asset.
So a category can be expected to change several attributes, and lifecycle doesn't say which.

## Draft condition attributes

Asset types are from the `ten_year_plan_category_asset_types` seed.
"Changed by" is the lifecycle we'd expect to move each attribute.

| Service | Asset type | Condition attribute | Measured as | Changed by |
|---|---|---|---|---|
| schools | school | seats | count | Program Expansion |
| schools | school, school support facility | building condition | rating | State of Good Repair |
| housing | housing | units | count | Program Expansion |
| housing | housing, public housing | unit condition | rating | State of Good Repair |
| parks and recreation | park | parkland | acres | Program Expansion |
| parks and recreation | park, recreation facility | amenity condition | rating | State of Good Repair |
| streets and sidewalks | street | pavement condition | rating | State of Good Repair |
| streets and sidewalks | street, sidewalk | pedestrian space | square feet | State of Good Repair (in redesigns) |
| streets and sidewalks | sidewalk, step street | walking surface condition | rating | State of Good Repair |
| streets and sidewalks | pedestrian ramp | ramp accessibility | share compliant | State of Good Repair |
| streets and sidewalks | traffic signal, pavement marking | traffic control condition | rating | State of Good Repair |
| streets and sidewalks | street light | lighting coverage | share of street lit | State of Good Repair |
| bridges | bridge | bridge condition | rating | State of Good Repair |
| sewers and stormwater | sewer, stormwater infrastructure | drainage capacity | design storm | Program Expansion |
| sewers and stormwater | sewer | sewer condition | rating | State of Good Repair, Programmatic Replacement |
| sewers and stormwater | green infrastructure | stormwater managed | impervious acres | Program Expansion |
| drinking water | water main, water service line | pipe condition | rating | Programmatic Replacement |
| drinking water | water supply tunnel, water supply system | supply redundancy | supply routes | Program Expansion |
| drinking water | dam, reservoir, watershed land | supply source condition | rating | Programmatic Replacement, State of Good Repair |
| drinking water | water filtration plant | treatment capacity | gallons per day | Program Expansion |
| emergency response | fire and EMS facility | facility condition | rating | State of Good Repair |

Some of these, like "rating", stand in for a measure an agency already uses that we haven't looked up.
Pedestrian space and parkland are the ones lifecycle hides: the money that changes them may sit in State of Good Repair categories.

## Open questions

- Where goals come from, and at what geography.
  The Citywide Statement of Needs (Charter §204) and community boards' District Needs Statements are the first places to look (see the [Capital Planning Glossary](../../docs/glossary-capital-planning.md)).
  The FY 2026-2035 Ten-Year Capital Strategy says the city is expanding the Citywide Statement of Needs to connect capital datasets, including the strategy and the community district needs process.
- Who backs "expected to change."
  These rows are our reading. For steering, a claim that a category changes an attribute probably needs OMB or the agency behind it.
- Which asset inventories exist for each attribute, and whether they have history.
- How to link a project to the assets it changes. About 20% of planned dollars in FISA 20260929 are in projects with geometry.

## Project-level sources

Sources that could say which assets a project changes, what kind of change, and where.
Counts are from NYC Open Data, queried in October 2026.

| Source | Ingest template | What it adds | Limits |
|---|---|---|---|
| Capital Projects Dashboard, Mayor's Office of Operations (`fb86-vt7u`, plus `gyhf-rsr3`, `95tx-snak`, `qj5n-h5qp`) | `moo_capital_projects_*` | Agency schedules joined to FMS IDs. 3,291 FMS IDs in the 202605 release have agency projects (`pid`), and one FMS ID can have many, each with its own name and description. Schedule history includes the reason for each forecast change. | Covers about 54% of FISA 20260929 planned dollars, matched on project ID. No HPD or DOE projects. 1,353 of 5,647 rows name a community district; the rest give a borough or "Citywide". |
| OMB Capital Project Detail Data (`wa2y-rh4b`, `s7yh-frbm`) | `omb_capital_project_dollars`, `omb_capital_project_milestones` | `scope_text` for 5,046 of 5,128 projects, `site_descr` for 1,835, and task milestones | Stopped after the October 2023 plan |
| SCA Capital Project Schedules and Budgets (`2xh6-psuq`) | `sca_capital_projects` | 13,437 projects at 1,445 school buildings, keyed by building ID, with a description of the work | `project_budget_amount` mixes in text codes. How its DSF numbers link to FMS IDs is unknown. |

Certificates to Proceed are OMB approvals every project needs before a contract can be registered, so they would cover every project. We haven't found them published.

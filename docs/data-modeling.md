# Data Modeling

How we model the data we make.

A data model can be described at three levels of abstraction: conceptual, logical, and physical ([MotherDuck glossary](https://motherduck.com/glossary/data-model/), [Juha Korpela](https://commonsensedata.substack.com/p/conceptual-logical-physical-the-truth)).
Each level is built from the one before it, and has to map back to it.

## Conceptual model

A conceptual model describes the things in a domain and how they relate to each other, before choosing how to store any data about them.

Our conceptual models are glossaries of a domain. Each one should:

- define each entity in one sentence, in plain words (`A lot is ...`)
- list relationships as verbs with cardinality (`Site spans 1 : 1..N Lot`), plus a diagram
- mark classifications as `is a` rows with no cardinality

As an example, the [urban planning glossary](./glossary-urban-planning.md) is a conceptual model of the built environment.

## Logical model

A logical model is the structure of the data for a use case: entities, their attributes and keys, and what one row represents (its grain).
It doesn't depend on the database or file format.

There are several methods for designing one, such as dimensional modeling, Data Vault, and 3NF.
Ours is adapted from Silja Märdla's talk "Beyond layers and medallions: An Explicit Data Modeling Framework" (dbt Summit 2026).

### Grain

A table's grain is what one row represents, stated as the set of keys that make a row unique: `lot`, `lot × release`, `community district × year`.

Every exposed table states its grain in its docs and has a uniqueness test on those keys.

### Exposed tables

Exposed tables are the ones people query directly, in any tool.
There are two kinds:

- **core**: the clean model of the data, read by engineers and internal analysts.
- **product**: tables shaped for one export or destination, read by the public.

Everything else is intermediate, and all logic lives there.

Exposed tables have finished values: nothing an analyst would have to recompute in their own tool.
Every column has a description, units where relevant, and its SSOT if it's a measure.

### Core shapes

Every `core` table has one of these shapes:

| Prefix | One row per | Example |
|---|---|---|
| `dim_` | thing that persists | a lot, a building, a zoning application |
| `fact_` | thing that happened | a filing, a status change, a permit issuance |
| `scd_` | version of an entity's attributes, bounded by `valid_from` / `valid_to` | a lot's zoning over time |
| `mart_` | combination of entities | community district × year |
| `ref_` | code in a code list, with its label | a land use code |

- **One `core` table per grain.** Everything about a grain is in one place.
- **No logic in `core`.** A `core` table selects and joins intermediate models at its grain. Calculations, filters, aggregations, and spatial operations all happen upstream.
- **Marts hold no original information.** Everything in a mart traces back to a `dim_`, `fact_`, or `scd_`. A mart is a convenience for a popular grain.
- **`dim_` and `fact_` entities are defined in a glossary.** Add new ones to the [conceptual model](#conceptual-model) before building tables for them.

### Intermediate models

Each `core` table is built from as many intermediate models as it needs, split by topic so each one is small and testable:

| Type | Holds | Name |
|---|---|---|
| spine | the grain's keys and basic attributes | `int_<grain>_spine` |
| features | descriptive attributes at this grain | `int_<grain>_<topic>_features` |
| metrics | measures aggregated to this grain from a finer one | `int_<grain>_<topic>_metrics` |

For example, a count of filings per lot is defined on `fact_filing`, aggregated in `int_lot_filing_metrics`, and exposed on `dim_lot`.

### Product tables

A `product` table shapes `core` tables for one destination.
It can filter rows, select and rename columns, choose a geometry column, and cast types.
It doesn't compute anything new.
The test: if a product table couldn't be generated from a config file, its logic belongs in an intermediate model.
Several product tables can share a grain.

### Single source of truth

A measure's single source of truth (SSOT) is the finest-grained table where the source records it.
The same measure on a coarser table uses the SSOT: same name, same definition, different grain.
Column docs say which table is the SSOT.
The same quantity from two sources is two measures with two names.
Comparing them is a feature or a test, not a silent pick.

### Additivity

| Type | Meaning | Examples |
|---|---|---|
| additive | sums across every dimension | counts, sums, areas of non-overlapping pieces |
| semi-additive | sums across some dimensions but not others, usually not across time | open applications at a point in time, or any measure in a table with release in its grain |
| non-additive | never sums | ratios, percentages, distinct counts, medians |

Keep non-additive measures out of marts that also hold additive ones, so tools can't sum them by accident.
For a ratio, also expose its numerator and denominator so it can be recomputed at any grain.

### Time and history

Many of our sources are versioned releases (e.g. `25v1`), not change events.
History can be modeled two ways:

| | Release snapshot | Slowly changing dimension (`scd_`) |
|---|---|---|
| Grain | entity × release | entity × version |
| Answers | "what did release X say?" | "what was true on date D?" |
| Rows | every entity, repeated every release | a new row only when something changed |

- If the source only gives release dates, an SCD built by diffing releases records when a change was published, not when it happened. Name the bounds for what they are (`first_release` / `last_release`).
- One `scd_` table per group of attributes that change together. Mixing groups multiplies rows and hides which attribute changed.
- Expose a current `dim_` next to any `scd_`, so nobody has to filter on `valid_to is null`.
- When analysts want "every version", build `mart_<entity>_release` from the `scd_` tables, and test that it reproduces the raw releases.

### Geospatial

- One `core` table owns an entity's canonical geometry. Other versions (clipped, simplified, reprojected) are features computed upstream, and their column names say how they differ.
- An overlay result is a `dim_` whose entity is the piece of the intersection (e.g. `dim_lot_zoning_district`, one row per piece of a lot in a district). It's the SSOT for overlap area. Expose `overlap_area` and the parent area, not just the share.

### Open questions

- Whether `mart_` reads well to analysts, or a `by_` prefix (`by_community_district_year`) is clearer.

## Physical model

A physical model is the implementation of the logical model: tables, column types, indexes, materialization, and file formats.
For dbt projects, see [dbt project conventions](./dbt-project-conventions.md).

### Exposed tables

Exposed tables are read by ArcGIS Pro, QGIS, Power BI, Python, R, and notebooks, so they have to work in the least capable of them:

- **Flat column types.** No structs, lists, maps, or JSON columns.
- **Stated CRS.** Every geometry column's description gives its CRS. A table that needs both EPSG:2263 and EPSG:4326 has two explicitly named geometry columns.
- **Portable column names.** Lowercase snake_case. `core` names don't have to fit a format's limits (like shapefile's 10 characters). The `product` table for that format renames them.

### Open questions

- Exposed file format and geometry encoding.

# Capital Planning Glossary

Terms and timelines for New York City's capital budget and the capital planning documents built from it.

For the physical things capital money is spent on, see **Capital Asset** in the [Urban Planning Glossary](glossary-urban-planning.md).

## Annual calendar

The city's budget follows the **Fiscal Year**, July 1 through June 30.
Dates with "by" are the latest dates set by the City Charter, as described by IBO.

| When | Event | Source |
|---|---|---|
| July 1 | Fiscal year begins | [Council][council], [IBO][ibo] |
| September | OMB publishes the Adopted **Capital Commitment Plan**, a four-year plan due within 90 days of the Capital Budget's adoption | [Comptroller][comptroller] |
| January (by Jan 16) | Mayor releases the **Preliminary Budget**, including the Preliminary Capital Budget | [IBO][ibo], [Comptroller][comptroller] |
| January | OMB publishes the Preliminary Capital Commitment Plan, a five-year plan | [Comptroller][comptroller] |
| January, odd-numbered years | Preliminary **Ten-Year Capital Strategy** | [Comptroller][comptroller] |
| March to April | Council analyzes the Preliminary Budget, holds public hearings, and releases its response | [Council][council] |
| April (by Apr 26) | Mayor presents the **Executive Budget**, including the Executive Capital Budget | [IBO][ibo], [Comptroller][comptroller] |
| April or May | OMB publishes the Executive Capital Commitment Plan, a five-year plan | [Comptroller][comptroller] |
| April or May, odd-numbered years | Final Ten-Year Capital Strategy | [IBO][ibo], [Comptroller][comptroller] |
| May to June (by Jun 5) | Council and Mayor negotiate, then Council votes to adopt the **Adopted Budget**, including the Capital Budget | [Council][council], [IBO][ibo] |
| June 30 | Fiscal year ends. The Adopted Budget must be in place before July 1 | [Council][council], [Comptroller][comptroller] |

Capital Commitment Plan months are when OMB typically publishes, not Charter deadlines.
Recent plans were dated 4/26/2023, 9/28/2023, 1/16/2024, 4/24/2024, 9/30/2024, 1/16/2025, and 5/1/2025 ([Comptroller][comptroller], Table 1).
The Adopted plan comes out after the fiscal year it starts in has already begun.

## Version labels

The same release can carry three different year labels:

- **Budgets** are named for the fiscal year they fund. The Preliminary Budget released in January covers the upcoming fiscal year ([IBO][ibo]).
- **The Comptroller** names Capital Commitment Plans for the fiscal year they're released in: the plan dated 5/1/2025 is the "FY 2025 May CCP", and the September 2025 plan is the "FY 2026 Adopted CCP" ([Comptroller][comptroller]).
- **CPDB** versions (`ccpversion`, and `version` in `products/cpdb/recipe.yml`) use the calendar year the plan was released in.

| Plan | FISA extract date | CPDB version | Comptroller label | Budget it accompanies |
|---|---|---|---|---|
| Adopted, fall 2025 | 2025-09-30 | `25adopt` | FY 2026 Adopted CCP | FY 2026 Adopted |
| Preliminary, early 2026 | 2026-02-18 | `26prelim` | FY 2026 January CCP | FY 2027 Preliminary |
| Executive, spring 2026 | 2026-05-18 | `26exec` | FY 2026 May CCP | FY 2027 Executive |

FISA extract dates come from `ingest_templates/fisa_capitalcommitments.yml` history.
CPDB and Comptroller labels differ only for Adopted plans.
Budget labels are one year ahead for Preliminary and Executive plans.

## Terms

### Time

**Fiscal Year**: A fiscal year is a 12-month accounting period that begins July 1 and ends the following June 30. Each fiscal year is named for the calendar year it ends in: fiscal year 2022 began in July 2021 and ended in June 2022. ([IBO][ibo])

### Budget stages

**Preliminary Budget**: The preliminary budget is the Mayor's proposed operating and capital expenditures and revenue forecast for the upcoming fiscal year plus the three after it, released by January 16. ([IBO][ibo])

**Executive Budget**: The executive budget is the Mayor's revised budget proposal for the upcoming fiscal year, presented to the Council by April 26 after the Council responds to the Preliminary Budget. ([IBO][ibo], [Council][council])

**Adopted Budget**: The adopted budget is the budget the Council votes to adopt after negotiating with the Mayor, due by June 5 and required before the fiscal year starts on July 1. ([IBO][ibo], [Council][council])

### Capital documents

**Capital Budget**: The capital budget sets the maximum capital appropriations, by budget line, that sponsor agencies can spend in the upcoming fiscal year, and projects appropriations for the three years after. It's released three times a year with the Preliminary, Executive, and Adopted Budgets. It works more as a spending limit than a plan. A project must be worth at least $50,000 to be included and, for most projects, have a period of probable usefulness of at least five years. ([Comptroller][comptroller], [IBO][ibo])

**Capital Commitment Plan**: The capital commitment plan (CCP) is OMB's schedule of **Planned Commitments** by FMS ID over the next four or five fiscal years. It's more granular than the Capital Budget and is the main capital planning document. OMB publishes it three times a year as a multi-volume PDF. CPDB is built from one release of it. ([Comptroller][comptroller], [CPDB data dictionary][cpdb-dd])

**Reserve for Unattained Commitments**: The reserve for unattained capital commitments is a lump-sum reduction in the Capital Commitment Plan that brings each fiscal year's planned commitments down to a realistic level, since OMB and agencies plan more than they can take on. It isn't broken out by project or agency, so project and agency totals from the plan add up to more than the plan's bottom line. ([Comptroller][comptroller])

**Ten-Year Capital Strategy**: The ten-year capital strategy (TYCS) is the Mayor's ten-year schedule of planned commitments, with the city's goals, key investments, and financing plan. A preliminary version comes out in January of odd-numbered years and the final version in April or May. It groups projects into agency-specific categories and lifecycle categories: state of good repair, program expansion, and programmatic replacement. It's separate from the Adopted Capital Budget and the Capital Commitment Plan. ([Comptroller][comptroller], [IBO][ibo], [Ten-Year Capital Strategy site][tycs])

### Money

**Appropriation**: An appropriation is the amount of money identified in the budget for spending by an agency. ([IBO][ibo])

**Capital Appropriation**: A capital appropriation is the amount of money allocated to a specific budget line in the Capital Budget. ([IBO][ibo])

**Allocation**: An allocation is a sum of money within an appropriation that is set aside for a specific purpose. ([IBO][ibo])

**Planned Commitment**: A planned commitment is a commitment an agency expects to make, listed in the Capital Commitment Plan with a planned month and year, amounts by funding source (city, state, federal, other), and what it pays for, such as design or construction. The planned month and year aren't very reliable for predicting which fiscal year it will actually happen in. ([CPDB data dictionary][cpdb-dd], [Comptroller][comptroller])

**Commitment**: A commitment is an awarded contract for capital spending, often covering several years, that has been registered with the City Comptroller. ([IBO][ibo], [Comptroller][comptroller])

**Liquidation**: A liquidation is a payment drawn down from a commitment's contract. It's the actual spending, and what Checkbook NYC's capital checks record. ([Comptroller][comptroller], [CSDB README](../products/checkbook/README.md))

### Projects and agencies

**Capital Project**: A capital project is the unit OMB plans commitments against, identified by an FMS ID (`maprojid` in CPDB, the managing agency code followed by a project ID). It has one or more planned commitments. An FMS ID isn't always a discrete project: some are holding codes, some are lump sums for a whole program or agency, and some are split by fiscal year. ([CPDB data dictionary][cpdb-dd], [Comptroller][comptroller])

**Holding Code**: A holding code is an FMS ID that holds planned commitments for a larger citywide program until they're moved to more specific FMS IDs, where the actual commitments and liquidations happen. FMS doesn't link a holding code to the IDs it's later spent under. ([Comptroller][comptroller])

**Budget Line**: A budget line is the Capital Budget's unit of appropriation, broader than an FMS ID. FMS IDs roll up to budget lines. ([Comptroller][comptroller], [CPDB data dictionary][cpdb-dd])

**Sponsor Agency**: A sponsor agency is the city agency sponsoring a capital project's planned commitments, derived from the budget line. The Capital Budget tells sponsor agencies how much they can spend. ([CPDB data dictionary][cpdb-dd], [Comptroller][comptroller])

**Managing Agency**: A managing agency is the city agency that manages a capital project, identified by the three-digit code at the start of its FMS ID. ([CPDB data dictionary][cpdb-dd])

**Financial Management System**: The Financial Management System (FMS) is the city's internal financial system and the authoritative source of capital financial data. It holds TYCS and CCP planned commitments plus actual commitments and liquidations by FMS ID. It doesn't hold project schedules or reasons for delays. ([Comptroller][comptroller])

## Relationships

| Entity | Relationship | Cardinality | Entity |
|---|---|---|---|
| Capital Commitment Plan | lists | 1 : 1..N | Planned Commitment |
| Capital Project | has | 1 : 1..N | Planned Commitment |
| Planned Commitment | funded by | 0..N : 1 | Budget Line |
| Budget Line | sponsored by | 0..N : 1 | Sponsor Agency |
| Capital Project | managed by | 0..N : 1 | Managing Agency |
| Commitment | paid out by | 1 : 0..N | Liquidation |

## Sources

- [NYC Comptroller: Flying Blind on Billions][comptroller] (December 2025), especially "What is the Capital Budget?"
- [IBO: Understanding New York City's Budget][ibo] (July 2021)
- [NYC Council: Budget Process][council]
- [DCP: CPDB data dictionary][cpdb-dd]
- [NYC Ten-Year Capital Strategy site][tycs]

[comptroller]: https://comptroller.nyc.gov/reports/flying-blind-on-billions-how-weak-capital-data-undermines-new-york-citys-infrastructure-investments/
[ibo]: https://www.ibo.nyc.gov/assets/ibo/downloads/pdf/budget-guides/understandingthebudget.pdf
[council]: https://council.nyc.gov/budget/process/
[cpdb-dd]: https://s-media.nyc.gov/agencies/dcp/assets/files/excel/data-tools/bytes/cpdb_data_dictionary.xlsx
[tycs]: https://accordion-smilodon-prwk.squarespace.com/

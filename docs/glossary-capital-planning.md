# Capital Planning Glossary

Terms and timelines for New York City's capital budget and the capital planning documents built from it.

For the physical things capital money is spent on, see **Capital Asset** in the [Urban Planning Glossary](glossary-urban-planning.md).

## Annual calendar

The city's budget follows the **Fiscal Year**, July 1 through June 30.
Dates with "by" are the latest dates set by the City Charter, as described by IBO.

| When | Event | Source |
|---|---|---|
| July 1 | Fiscal year begins | [Council][council], [IBO][ibo] |
| January (by Jan 16) | Mayor releases the **Preliminary Budget**, including proposed capital expenditures | [IBO][ibo] |
| January | OMB publishes the Preliminary **Capital Commitment Plan** | [CPDB data dictionary][cpdb-dd] |
| January, odd-numbered years | OMB publishes the Preliminary **Ten-Year Capital Strategy** | [OMB][ptyp-25] |
| March to April | Council analyzes the Preliminary Budget, holds public hearings, and releases its response | [Council][council] |
| April (by Apr 26) | Mayor presents the **Executive Budget** | [IBO][ibo] |
| April | OMB publishes the Executive Capital Commitment Plan | [CPDB data dictionary][cpdb-dd] |
| April (by Apr 26), odd-numbered years | Mayor issues the Ten-Year Capital Strategy | [IBO][ibo] |
| May to June (by Jun 5) | Council and Mayor negotiate, then Council votes to adopt the **Adopted Budget** | [Council][council], [IBO][ibo] |
| June 30 | Fiscal year ends. The Adopted Budget must be in place before July 1 | [Council][council] |
| September | OMB publishes the Adopted Capital Commitment Plan | [CPDB data dictionary][cpdb-dd] |

The Capital Commitment Plan months are when OMB "generally" publishes, not Charter deadlines.
The Adopted Capital Commitment Plan comes out a few months after the budget is adopted, early in the next fiscal year.

## Terms

### Time

**Fiscal Year**: A fiscal year is a 12-month accounting period that begins July 1 and ends the following June 30. Each fiscal year is named for the calendar year it ends in: fiscal year 2022 began in July 2021 and ended in June 2022. ([IBO][ibo])

### Budget stages

**Preliminary Budget**: The preliminary budget is the Mayor's proposed operating and capital expenditures and revenue forecast for the upcoming fiscal year plus the three after it, released by January 16. ([IBO][ibo])

**Executive Budget**: The executive budget is the Mayor's revised budget proposal for the upcoming fiscal year, presented to the Council by April 26 after the Council responds to the Preliminary Budget. ([IBO][ibo], [Council][council])

**Adopted Budget**: The adopted budget is the budget the Council votes to adopt after negotiating with the Mayor, due by June 5 and required before the fiscal year starts on July 1. ([IBO][ibo], [Council][council])

### Capital documents

**Capital Budget**: The capital budget is a budget covering one fiscal year that funds physical infrastructure, separate from the expense budget. A project must be worth at least $50,000 to be included and, for most projects, have a period of probable usefulness of at least five years. ([IBO][ibo])

**Capital Program**: The capital program is a multiyear plan of the funds needed for the current fiscal year and the next three, for projects already underway and new projects started in the Capital Budget. ([IBO][ibo])

**Capital Commitment Plan**: The capital commitment plan is OMB's plan of **Planned Commitments** by capital project. OMB publishes it three times a year, alongside the Preliminary, Executive, and Adopted Capital Budgets. CPDB is built from one release of it, identified by `ccpversion` (for example `26exec`). ([CPDB data dictionary][cpdb-dd])

**Ten-Year Capital Strategy**: The ten-year capital strategy is the Mayor's plan for developing the city's capital facilities over the next decade, issued in odd-numbered years. It's separate from the Adopted Capital Budget and the Capital Commitment Plan. ([IBO][ibo], [Ten-Year Capital Strategy site][tycs])

### Money

**Appropriation**: An appropriation is the amount of money identified in the budget for spending by an agency. ([IBO][ibo])

**Capital Appropriation**: A capital appropriation is the amount of money allocated to a specific budget line in the Capital Budget. ([IBO][ibo])

**Allocation**: An allocation is a sum of money within an appropriation that is set aside for a specific purpose. ([IBO][ibo])

**Commitment**: A commitment is an awarded contract for capital budget spending, often covering several years, that has been registered with the City Comptroller. ([IBO][ibo])

**Planned Commitment**: A planned commitment is a commitment an agency expects to make, listed in the Capital Commitment Plan with a planned month and year, amounts by funding source (city, state, federal, other), and what it pays for, such as design or construction. It's the row grain of CPDB's commitments dataset. ([CPDB data dictionary][cpdb-dd])

## Sources

- [IBO: Understanding New York City's Budget][ibo] (July 2021)
- [NYC Council: Budget Process][council]
- [DCP: CPDB data dictionary][cpdb-dd]
- [OMB: Preliminary Ten-Year Capital Strategy, Fiscal Years 2026-2035][ptyp-25]
- [NYC Ten-Year Capital Strategy site][tycs]

[ibo]: https://www.ibo.nyc.gov/assets/ibo/downloads/pdf/budget-guides/understandingthebudget.pdf
[council]: https://council.nyc.gov/budget/process/
[cpdb-dd]: https://s-media.nyc.gov/agencies/dcp/assets/files/excel/data-tools/bytes/cpdb_data_dictionary.xlsx
[ptyp-25]: https://www.nyc.gov/assets/omb/downloads/pdf/jan25/ptyp1-25.pdf
[tycs]: https://accordion-smilodon-prwk.squarespace.com/

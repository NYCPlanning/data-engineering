# db-cpdb

![capital spending](https://github.com/NYCPlanning/db-cpdb/workflows/capital%20spending/badge.svg) ![Build/Run CPDB](https://github.com/NYCPlanning/db-cpdb/workflows/CI%20test/badge.svg)

## Instructions

1. All the relevant commands for running CPDB is wrapped in the cli bash script `./cpdb.sh`, please read the file for more details.
2. The capital spending scraping process should be done right after we load `fisa_capitalcommitments` to data library. A separate bash script will import  `fisa_capitalcommitments` to bigquery and we will create export the capital spending table `cpdb_capital_spending` via a bigquery command (see `bash/11_spending.sh` for more details)

    > Note that this process is triggered via workflow dispatch

3. Make sure to edit the `version.env` file to reflect the current fiscal year.

    > The fiscal year begins on July 1st of one calendar year and ends on June 30th of the following calendar year ([Mayor's Office of Management and Budget: When does the City's fiscal year begin and end?](https://www1.nyc.gov/site/omb/faq/frequently-asked-questions.page#:~:text=The%20fiscal%20year%20begins%20on,of%20the%20following%20calendar%20year)).
    > The output files will be stored in subfolders named after branches.

4. Since CPDB is still a private database. You can generate a pre-signed sharable link using the `./cpdb.sh share` command. Run `./cpdb.sh share --help` to see instructions.

> Note: the url will only be valid for 7 days.

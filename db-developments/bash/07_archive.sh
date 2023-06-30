#!/bin/bash
source bash/config.sh

display "archive DevDB"
archive_devdb EXPORT_devdb developments

display "archive HousingDB"
archive_devdb EXPORT_housing dcp_housing

# archive yearly_unitchange table
# archive yearly_unitchange yearly_unitchange
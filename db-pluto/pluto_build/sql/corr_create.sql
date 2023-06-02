-- add field to pluto to indicate a value has been edited
ALTER TABLE pluto
ADD COLUMN dcpedited text;

DROP TABLE IF EXISTS pluto_changes_not_applied;
CREATE TABLE pluto_changes_not_applied (LIKE pluto_input_research INCLUDING ALL);

DROP TABLE IF EXISTS pluto_changes_applied;
CREATE TABLE pluto_changes_applied (LIKE pluto_input_research INCLUDING ALL);

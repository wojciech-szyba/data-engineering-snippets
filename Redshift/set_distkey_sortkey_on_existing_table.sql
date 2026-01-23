ALTER TABLE schema_name.table_name RENAME TO schema_name.table_name_no_sortkey;

CREATE TABLE schema_name.table_name
DISTSTYLE KEY
DISTKEY (...)
SORTKEY (...)
AS
SELECT *
FROM schema_name.table_name_no_sortkey;


SELECT * FROM schema_name.table_name_no_sortkey
MINUS
SELECT * FROM schema_name.table_name;


DROP TABLE schema_name.table_name_no_sortkey;

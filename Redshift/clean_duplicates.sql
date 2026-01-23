CREATE TABLE <schema_name>.<table_name>_bck AS SELECT * FROM <schema_name>.<table_name>;

CREATE TABLE <schema_name>.<table_name>_duplicates_filtered  AS 
 SELECT * FROM <schema_name>.<table_name>
WHERE 1=1
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY <sort_column> DESC) = 1;

TRUNCATE TABLE <schema_name>.<table_name>;

INSERT INTO <schema_name>.<table_name> SELECT * FROM <schema_name>.<table_name>_duplicates_filtered ;

COMMIT;


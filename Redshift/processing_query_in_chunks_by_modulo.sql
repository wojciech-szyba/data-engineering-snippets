SELECT id, col1, col2 FROM table_name
--WHERE MOD(CHECKSUM(table_name.id),4) = 0
--WHERE MOD(CHECKSUM(table_name.id),4) = 1
WHERE MOD(CHECKSUM(table_name.id),4) = 2
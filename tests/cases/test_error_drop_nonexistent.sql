-- DROP TABLE on nonexistent table should report table name
-- input:
DROP TABLE nonexistent;
-- expected output:
ERROR:  table 'nonexistent' does not exist
-- expected status: 1

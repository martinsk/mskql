-- DROP TABLE IF EXISTS on non-existent table should succeed silently
-- setup:
-- input:
DROP TABLE IF EXISTS no_such_table;
-- expected output:
DROP TABLE
-- expected status: 0

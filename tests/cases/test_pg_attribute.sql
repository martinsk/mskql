-- pg_catalog.pg_attribute lists columns for a table
-- setup:
CREATE TABLE t1 (id INT NOT NULL, name TEXT)
-- input:
SELECT attrelid, attname, atttypid, attnum, attnotnull FROM pg_catalog.pg_attribute WHERE attrelid = 16384 ORDER BY attnum
-- expected output:
16384|id|23|1|t
16384|name|25|2|f

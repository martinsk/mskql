-- BUG: ALTER TABLE ADD COLUMN with duplicate name should error
-- setup:
CREATE TABLE t (id INT, name TEXT);
-- input:
ALTER TABLE t ADD COLUMN name TEXT;
-- expected output:
ERROR:  column "name" of relation "t" already exists
-- expected status: 0

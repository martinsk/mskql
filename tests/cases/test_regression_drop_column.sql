-- BUG: ALTER TABLE DROP COLUMN does not actually remove the column
-- setup:
CREATE TABLE t (id INT, name TEXT, val INT);
INSERT INTO t VALUES (1, 'hello', 100);
ALTER TABLE t DROP COLUMN name;
-- input:
SELECT * FROM t;
-- expected output:
1|100
-- expected status: 0

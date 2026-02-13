-- CREATE TABLE IF NOT EXISTS then insert works
-- setup:
CREATE TABLE t1 (id INT, name TEXT)
INSERT INTO t1 VALUES (1, 'first')
CREATE TABLE IF NOT EXISTS t1 (id INT, name TEXT)
INSERT INTO t1 VALUES (2, 'second')
-- input:
SELECT * FROM t1 ORDER BY id
-- expected output:
1|first
2|second

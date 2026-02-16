-- plan: WHERE with OR combining IS NULL and comparison
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT);
INSERT INTO t1 VALUES (1, 'alice', 10), (2, 'bob', NULL), (3, 'charlie', 30), (4, 'dave', NULL);
-- input:
SELECT name FROM t1 WHERE val IS NULL OR val > 20 ORDER BY name;
-- expected output:
bob
charlie
dave
-- expected status: 0

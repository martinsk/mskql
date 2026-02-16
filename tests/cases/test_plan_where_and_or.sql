-- plan: WHERE with AND and OR combined
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT, active INT);
INSERT INTO t1 VALUES (1, 'alice', 10, 1), (2, 'bob', 20, 0), (3, 'charlie', 30, 1), (4, 'dave', 40, 0);
-- input:
SELECT name FROM t1 WHERE active = 1 AND (val = 10 OR val = 30) ORDER BY name;
-- expected output:
alice
charlie
-- expected status: 0

-- plan: WHERE with OR condition
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT);
INSERT INTO t1 VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30), (4, 'dave', 40);
-- input:
SELECT name FROM t1 WHERE val = 10 OR val = 30 ORDER BY name;
-- expected output:
alice
charlie
-- expected status: 0

-- plan: WHERE with OR on text columns
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT);
INSERT INTO t1 VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30), (4, 'dave', 40);
-- input:
SELECT val FROM t1 WHERE name = 'alice' OR name = 'dave' ORDER BY val;
-- expected output:
10
40
-- expected status: 0

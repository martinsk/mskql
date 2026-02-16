-- plan: WHERE with OR on different columns
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT);
INSERT INTO t1 VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30), (4, 'dave', 40);
-- input:
SELECT name, val FROM t1 WHERE id = 1 OR val >= 30 ORDER BY id;
-- expected output:
alice|10
charlie|30
dave|40
-- expected status: 0

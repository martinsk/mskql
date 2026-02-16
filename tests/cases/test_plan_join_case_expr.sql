-- plan: JOIN with CASE expression projection
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, val INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t2 VALUES (1, 100), (2, 50);
-- input:
SELECT t1.name, CASE WHEN t2.val >= 100 THEN 'high' ELSE 'low' END AS tier FROM t1 JOIN t2 ON t1.id = t2.fk ORDER BY t1.name;
-- expected output:
alice|high
bob|low
-- expected status: 0

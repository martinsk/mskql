-- plan: WHERE with LIKE combined with AND
-- setup:
CREATE TABLE t1 (id INT, name TEXT, active INT);
INSERT INTO t1 VALUES (1, 'alice', 1), (2, 'bob', 0), (3, 'alicia', 1), (4, 'dave', 1);
-- input:
SELECT id FROM t1 WHERE name LIKE 'ali%' AND active = 1 ORDER BY id;
-- expected output:
1
3
-- expected status: 0

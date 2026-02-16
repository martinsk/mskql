-- plan: WHERE with ILIKE (case-insensitive)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'Alice'), (2, 'BOB'), (3, 'carol'), (4, 'DAVE');
-- input:
SELECT id FROM t1 WHERE name ILIKE 'bob' ORDER BY id;
-- expected output:
2
-- expected status: 0

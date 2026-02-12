-- LIKE with combined _ and % patterns
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'abc'), (2, 'aXc'), (3, 'ac'), (4, 'aXYc');
-- input:
SELECT id FROM t1 WHERE name LIKE 'a_c' ORDER BY id;
-- expected output:
1
2
-- expected status: 0

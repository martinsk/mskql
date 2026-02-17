-- implicit comma join should include columns from both tables
-- setup:
CREATE TABLE t1 (id INT, val INT);
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 10), (2, 20);
INSERT INTO t2 VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT t1.id, t1.val, t2.name FROM t1, t2 WHERE t1.id = t2.id ORDER BY t1.id;
-- expected output:
1|10|alice
2|20|bob
-- expected status: 0

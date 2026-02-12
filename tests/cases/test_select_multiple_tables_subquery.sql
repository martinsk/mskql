-- scalar subquery in SELECT list
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (ref_id INT, amount INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t2 VALUES (1, 50), (1, 30), (2, 100);
-- input:
SELECT t1.id, t1.name, (SELECT SUM(amount) FROM t2 WHERE t2.ref_id = t1.id) FROM t1 ORDER BY t1.id;
-- expected output:
1|alice|80
2|bob|100
-- expected status: 0

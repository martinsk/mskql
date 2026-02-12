-- correlated subquery in WHERE with expression
-- setup:
CREATE TABLE t1 (id INT, val INT);
CREATE TABLE t2 (id INT, ref_id INT, amount INT);
INSERT INTO t1 VALUES (1, 100), (2, 200);
INSERT INTO t2 VALUES (1, 1, 50), (2, 1, 60), (3, 2, 10);
-- input:
SELECT t1.id, t1.val FROM t1 WHERE t1.val > (SELECT SUM(amount) FROM t2 WHERE t2.ref_id = t1.id) ORDER BY t1.id;
-- expected output:
2|200
-- expected status: 0

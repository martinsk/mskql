-- plan: JOIN with GROUP BY and HAVING clause
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, amount INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 100), (1, 200), (2, 50), (2, 30), (3, 400);
-- input:
SELECT t1.name, SUM(t2.amount) AS total FROM t1 JOIN t2 ON t1.id = t2.fk GROUP BY t1.name HAVING SUM(t2.amount) > 100 ORDER BY t1.name;
-- expected output:
alice|300
charlie|400
-- expected status: 0

-- plan: LEFT JOIN with GROUP BY and aggregate (COUNT includes NULLs from outer)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, val INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 10), (1, 20), (3, 30);
-- input:
SELECT t1.name, COUNT(t2.val) AS cnt FROM t1 LEFT JOIN t2 ON t1.id = t2.fk GROUP BY t1.name ORDER BY t1.name;
-- expected output:
alice|2
bob|0
charlie|1
-- expected status: 0

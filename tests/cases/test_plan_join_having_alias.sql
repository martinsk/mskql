-- plan: JOIN with GROUP BY and HAVING using aggregate alias
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, amount INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 100), (1, 200), (2, 50), (2, 30), (3, 400);
-- input:
SELECT t1.name, COUNT(*) AS cnt FROM t1 JOIN t2 ON t1.id = t2.fk GROUP BY t1.name HAVING cnt >= 2 ORDER BY t1.name;
-- expected output:
alice|2
bob|2
-- expected status: 0

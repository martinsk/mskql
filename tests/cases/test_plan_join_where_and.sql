-- plan: JOIN with compound AND WHERE clause
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, amount INT, active INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 300, 1), (2, 100, 1), (3, 200, 0);
-- input:
SELECT t1.name, t2.amount FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.amount > 150 AND t2.active = 1 ORDER BY t1.name;
-- expected output:
alice|300
-- expected status: 0

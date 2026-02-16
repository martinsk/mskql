-- plan: LEFT JOIN with COALESCE expression projection
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, val INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 100), (3, 300);
-- input:
SELECT t1.name, COALESCE(t2.val, 0) AS val FROM t1 LEFT JOIN t2 ON t1.id = t2.fk ORDER BY t1.name;
-- expected output:
alice|100
bob|0
charlie|300
-- expected status: 0

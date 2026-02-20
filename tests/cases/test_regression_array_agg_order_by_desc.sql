-- BUG: ARRAY_AGG with ORDER BY DESC ignores the DESC direction
-- setup:
CREATE TABLE t (id INT, grp TEXT, val INT);
INSERT INTO t VALUES (1, 'A', 30), (2, 'A', 10), (3, 'A', 20);
-- input:
SELECT grp, ARRAY_AGG(val ORDER BY val DESC) FROM t GROUP BY grp;
-- expected output:
A|{30,20,10}
-- expected status: 0

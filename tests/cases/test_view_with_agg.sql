-- CREATE VIEW with aggregate and SELECT from it
-- setup:
CREATE TABLE t1 (grp TEXT, val INT);
INSERT INTO t1 VALUES ('a', 10), ('a', 20), ('b', 30);
CREATE VIEW v1 AS SELECT grp, SUM(val) AS total FROM t1 GROUP BY grp;
-- input:
SELECT * FROM v1 ORDER BY grp;
-- expected output:
a|30
b|30
-- expected status: 0

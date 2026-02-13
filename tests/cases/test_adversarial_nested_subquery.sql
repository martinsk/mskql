-- adversarial: deeply nested scalar subqueries
-- setup:
CREATE TABLE t_ns (v INT);
INSERT INTO t_ns VALUES (1);
-- input:
SELECT (SELECT (SELECT (SELECT v FROM t_ns) FROM t_ns) FROM t_ns) FROM t_ns;
-- expected output:
1

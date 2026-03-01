-- PLAN_SUBQUERY: FROM subquery with alias column access
-- setup:
CREATE TABLE t (id INT, name TEXT, score INT);
INSERT INTO t VALUES (1, 'alice', 90), (2, 'bob', 70), (3, 'carol', 85);
-- input:
SELECT * FROM (SELECT id, name FROM t WHERE score >= 85) winners ORDER BY id;
-- expected output:
1|alice
3|carol
-- expected status: 0

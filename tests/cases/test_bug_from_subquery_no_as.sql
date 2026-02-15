-- bug: FROM subquery without AS keyword fails with "expected AS alias after FROM subquery"
-- setup:
CREATE TABLE t_fsq (id INT, val INT);
INSERT INTO t_fsq VALUES (1, 50), (2, 30), (3, 40), (4, 10), (5, 20);
-- input:
SELECT sq.id, sq.val FROM (SELECT id, val FROM t_fsq ORDER BY val LIMIT 3) sq ORDER BY sq.id;
-- expected output:
2|30
4|10
5|20

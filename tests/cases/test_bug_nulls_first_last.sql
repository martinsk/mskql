-- bug: NULLS FIRST / NULLS LAST in ORDER BY is ignored
-- setup:
CREATE TABLE t_nfl (id INT, val INT);
INSERT INTO t_nfl VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20);
-- input:
SELECT id, val FROM t_nfl ORDER BY val ASC NULLS FIRST;
-- expected output:
2|
4|
3|10
5|20
1|30

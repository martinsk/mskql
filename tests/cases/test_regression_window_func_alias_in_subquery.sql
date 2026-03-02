-- Bug: window function column alias not accessible in outer query when subquery is in FROM
-- SELECT rn FROM (SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM t) sub
-- returns error: column "rn" does not exist
-- In PostgreSQL this should work fine
-- setup:
CREATE TABLE t_win (n INT);
INSERT INTO t_win VALUES (1),(2),(3);
-- input:
SELECT rn FROM (SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM t_win) sub ORDER BY rn;
-- expected output:
1
2
3
-- expected status: 0

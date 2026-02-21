-- BUG: Double-nested subquery in FROM crashes the server
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT * FROM (SELECT * FROM (SELECT id, val FROM t WHERE val > 10) inner_sub) outer_sub ORDER BY id;
-- expected output:
2|20
3|30
-- expected status: 0

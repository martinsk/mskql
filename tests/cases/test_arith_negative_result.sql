-- arithmetic producing negative result
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 5, 10);
-- input:
SELECT id, a - b FROM t1;
-- expected output:
1|-5
-- expected status: 0

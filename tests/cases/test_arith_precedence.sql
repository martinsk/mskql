-- arithmetic operator precedence: multiplication before addition
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT, c INT);
INSERT INTO t1 (id, a, b, c) VALUES (1, 2, 3, 4);
-- input:
SELECT id, a + b * c FROM t1;
-- expected output:
1|14
-- expected status: 0

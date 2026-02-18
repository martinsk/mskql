-- adversarial: NULL in one indexed column, SELECT with non-NULL conditions
-- setup:
CREATE TABLE t_amnk (a INT, b INT, val TEXT);
CREATE INDEX idx_amnk ON t_amnk (a, b);
INSERT INTO t_amnk VALUES (1, NULL, 'null_b');
INSERT INTO t_amnk VALUES (1, 10, 'has_b');
INSERT INTO t_amnk VALUES (NULL, 10, 'null_a');
-- input:
SELECT val FROM t_amnk WHERE a = 1 AND b = 10;
-- expected output:
has_b

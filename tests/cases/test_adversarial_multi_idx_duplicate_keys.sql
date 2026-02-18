-- adversarial: duplicate composite key values, all rows returned
-- setup:
CREATE TABLE t_amdk (a INT, b INT, val TEXT);
CREATE INDEX idx_amdk ON t_amdk (a, b);
INSERT INTO t_amdk VALUES (1, 2, 'first');
INSERT INTO t_amdk VALUES (1, 2, 'second');
INSERT INTO t_amdk VALUES (1, 2, 'third');
-- input:
SELECT val FROM t_amdk WHERE a = 1 AND b = 2 ORDER BY val;
-- expected output:
first
second
third

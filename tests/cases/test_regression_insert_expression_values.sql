-- bug: INSERT with expression in VALUES fails with "expected ',' or ')'"
-- setup:
CREATE TABLE t_iev (id INT, val INT);
-- input:
INSERT INTO t_iev VALUES (1, 2 + 3);
SELECT id, val FROM t_iev;
-- expected output:
INSERT 0 1
1|5

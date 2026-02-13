-- adversarial: bigint arithmetic near overflow boundaries
-- setup:
CREATE TABLE t_big (v BIGINT);
INSERT INTO t_big VALUES (9223372036854775807);
-- input:
SELECT v + 0 FROM t_big;
-- expected output:
9.22337e+18

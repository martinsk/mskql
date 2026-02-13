-- adversarial: COALESCE where all arguments are NULL
-- setup:
CREATE TABLE t_cn2 (a INT, b INT, c INT);
INSERT INTO t_cn2 VALUES (NULL, NULL, NULL);
-- input:
SELECT COALESCE(a, b, c) FROM t_cn2;
-- expected output:


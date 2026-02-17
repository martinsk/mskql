-- adversarial: modulo by zero should error
-- setup:
CREATE TABLE t_mod (a INT, b INT);
INSERT INTO t_mod VALUES (10, 0);
INSERT INTO t_mod VALUES (7, 3);
-- input:
SELECT a % b FROM t_mod;
-- expected output:
ERROR:  division by zero

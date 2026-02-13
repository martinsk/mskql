-- adversarial: deeply nested arithmetic expressions — tests recursive eval_expr stack depth
-- setup:
CREATE TABLE t_dne (v INT);
INSERT INTO t_dne VALUES (1);
-- input:
SELECT ((((((((((v + 1) + 1) + 1) + 1) + 1) + 1) + 1) + 1) + 1) + 1) FROM t_dne;
-- expected output:
11

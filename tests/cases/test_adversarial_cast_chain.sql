-- adversarial: chained casts should not crash or leak
-- setup:
CREATE TABLE t_cc (v INT);
INSERT INTO t_cc VALUES (42);
-- input:
SELECT v::TEXT::INT::TEXT::INT FROM t_cc;
-- expected output:
42

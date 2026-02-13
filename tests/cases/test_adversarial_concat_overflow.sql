-- adversarial: CONCAT with many arguments — tests 4096-byte stack buffer in FUNC_CONCAT
-- NOTE: 40+ args causes stack overflow crash in eval_expr (see test_adversarial_concat_crash.sql)
-- Using 10 args here to stay safe. 100-char string * 10 = 1000 chars.
-- setup:
CREATE TABLE t_co (s TEXT);
INSERT INTO t_co VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
-- input:
SELECT LENGTH(CONCAT(s,s,s,s,s,s,s,s,s,s)) FROM t_co;
-- expected output:
1000

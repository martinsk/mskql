-- adversarial: LPAD with very large target length — potential OOM
-- setup:
CREATE TABLE t_lp (s TEXT);
INSERT INTO t_lp VALUES ('x');
-- input:
SELECT LENGTH(LPAD(s, 100000, 'y')) FROM t_lp;
-- expected output:
100000

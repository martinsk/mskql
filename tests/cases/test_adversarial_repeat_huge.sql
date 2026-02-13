-- adversarial: REPEAT with large count — potential OOM or huge allocation
-- setup:
CREATE TABLE t_rep (s TEXT);
INSERT INTO t_rep VALUES ('x');
-- input:
SELECT LENGTH(REPEAT(s, 100000)) FROM t_rep;
-- expected output:
100000

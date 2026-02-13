-- adversarial: REPLACE with empty search string — should return original (not infinite loop)
-- setup:
CREATE TABLE t_re (s TEXT);
INSERT INTO t_re VALUES ('hello');
-- input:
SELECT REPLACE(s, '', 'x') FROM t_re;
-- expected output:
hello

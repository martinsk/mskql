-- adversarial: string functions on empty strings
-- setup:
CREATE TABLE t_empty_str (s TEXT);
INSERT INTO t_empty_str VALUES ('');
-- input:
SELECT LENGTH(s), UPPER(s), LOWER(s), REVERSE(s), TRIM(s), INITCAP(s) FROM t_empty_str;
-- expected output:
0|||||

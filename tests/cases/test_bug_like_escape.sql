-- bug: LIKE with backslash escape for literal % does not match
-- setup:
CREATE TABLE t_lesc (val TEXT);
INSERT INTO t_lesc VALUES ('100%'), ('50%'), ('abc'), ('a%b');
-- input:
SELECT val FROM t_lesc WHERE val LIKE '%\%%' ORDER BY val;
-- expected output:
100%
50%
a%b

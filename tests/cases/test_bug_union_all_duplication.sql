-- bug: UNION ALL duplicates both sides (returns 2x expected rows)
-- setup:
CREATE TABLE t_ua (val TEXT);
INSERT INTO t_ua VALUES ('a'), ('b'), ('c');
-- input:
SELECT val FROM t_ua WHERE val = 'a' UNION ALL SELECT val FROM t_ua WHERE val = 'b';
-- expected output:
a
b

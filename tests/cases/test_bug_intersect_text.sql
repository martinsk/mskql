-- bug: INTERSECT on TEXT columns returns empty instead of common rows
-- setup:
CREATE TABLE t_ix (val TEXT);
INSERT INTO t_ix VALUES ('a'), ('b'), ('c');
-- input:
SELECT val FROM t_ix WHERE val IN ('a', 'b') INTERSECT SELECT val FROM t_ix WHERE val IN ('b', 'c');
-- expected output:
b

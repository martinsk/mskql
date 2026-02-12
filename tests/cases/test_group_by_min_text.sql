-- GROUP BY with MIN on text column
-- setup:
CREATE TABLE t1 (grp TEXT, name TEXT);
INSERT INTO t1 VALUES ('a', 'charlie'), ('a', 'alice'), ('b', 'bob'), ('b', 'dave');
-- input:
SELECT grp, MIN(name) FROM t1 GROUP BY grp ORDER BY grp;
-- expected output:
a|alice
b|bob
-- expected status: 0

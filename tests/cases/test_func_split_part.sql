-- SPLIT_PART function
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, 'a.b.c'), (2, 'x-y-z');
-- input:
SELECT id, SPLIT_PART(val, '.', 2), SPLIT_PART(val, '-', 1) FROM t1 ORDER BY id;
-- expected output:
1|b|a.b.c
2||x
-- expected status: 0

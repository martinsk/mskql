-- NOT IN with NULL in the list - SQL standard says this should return no rows
-- when the list contains NULL (because x != NULL is UNKNOWN)
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1), (2), (3);
-- input:
SELECT id FROM t1 WHERE id NOT IN (1, NULL) ORDER BY id;
-- expected output:
2
3
-- expected status: 0

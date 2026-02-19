-- bug: composite PRIMARY KEY (a, b) does not reject duplicate key inserts
-- setup:
CREATE TABLE t_multi_pk (a INT, b INT, c INT, PRIMARY KEY (a, b));
INSERT INTO t_multi_pk VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30);
-- input:
INSERT INTO t_multi_pk VALUES (1, 1, 40);
-- expected output:
ERROR:  duplicate key value violates unique constraint
-- expected status: 0

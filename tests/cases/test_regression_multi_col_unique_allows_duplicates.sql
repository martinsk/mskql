-- bug: composite UNIQUE(a, b) constraint does not reject duplicate key inserts
-- setup:
CREATE TABLE t_multi_uniq (a INT, b INT, c INT, UNIQUE(a, b));
INSERT INTO t_multi_uniq VALUES (1, 1, 10), (1, 2, 20);
-- input:
INSERT INTO t_multi_uniq VALUES (1, 1, 30);
-- expected output:
ERROR:  duplicate key value violates unique constraint
-- expected status: 0

-- Test vectorized IN list
CREATE TABLE t_in (a INT, b BIGINT);
INSERT INTO t_in VALUES (1, 10);
INSERT INTO t_in VALUES (2, 20);
INSERT INTO t_in VALUES (3, 30);
INSERT INTO t_in VALUES (4, 40);
INSERT INTO t_in VALUES (5, 50);

-- INT IN (lit, lit, ...)
SELECT a, a IN (1, 3, 5) AS in_list FROM t_in ORDER BY a;

-- NOT IN
SELECT a, a NOT IN (2, 4) AS not_in_list FROM t_in ORDER BY a;

-- BIGINT IN
SELECT b, b IN (20, 40) AS in_big FROM t_in ORDER BY b;

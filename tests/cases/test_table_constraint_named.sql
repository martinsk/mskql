-- table-level CONSTRAINT name PRIMARY KEY (a, b)
-- setup:
CREATE TABLE t_tcn (a INT, b INT, val TEXT, CONSTRAINT pk_tcn PRIMARY KEY (a, b));
INSERT INTO t_tcn VALUES (5, 6, 'found');
INSERT INTO t_tcn VALUES (5, 7, 'other');
-- input:
SELECT val FROM t_tcn WHERE a = 5 AND b = 6;
-- expected output:
found

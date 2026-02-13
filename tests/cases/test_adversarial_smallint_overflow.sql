-- adversarial: SMALLINT overflow — inserting value beyond 16-bit range
-- setup:
CREATE TABLE t_so (v SMALLINT);
INSERT INTO t_so VALUES (32767);
-- input:
SELECT v + 1 FROM t_so;
-- expected output:
32768

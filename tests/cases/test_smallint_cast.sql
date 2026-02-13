-- SMALLINT cast: SELECT 100::smallint
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1);
-- input:
SELECT 100::smallint FROM t1;
-- expected output:
100
-- expected status: 0

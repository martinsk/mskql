-- SMALLINT basic: CREATE TABLE, INSERT, SELECT
-- setup:
CREATE TABLE t1 (id SMALLINT, val SMALLINT);
INSERT INTO t1 (id, val) VALUES (1, 100);
INSERT INTO t1 (id, val) VALUES (2, -100);
INSERT INTO t1 (id, val) VALUES (3, 32767);
INSERT INTO t1 (id, val) VALUES (4, -32768);
-- input:
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
1|100
2|-100
3|32767
4|-32768
-- expected status: 0

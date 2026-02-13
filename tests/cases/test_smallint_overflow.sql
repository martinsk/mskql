-- SMALLINT overflow: INSERT value > 32767 should error
-- setup:
CREATE TABLE t1 (id INT, val SMALLINT);
-- input:
INSERT INTO t1 (id, val) VALUES (1, 32768);
-- expected output:
ERROR:  smallint out of range for column 'val'
-- expected status: 1

-- ARRAY_AGG includes NULL values
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, NULL);
INSERT INTO t1 (id, name) VALUES (3, 'carol');
-- input:
SELECT ARRAY_AGG(name) FROM t1;
-- expected output:
{alice,NULL,carol}
-- expected status: 0

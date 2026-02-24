-- vector: COUNT with vector column table
-- setup:
CREATE TABLE t_vcnt (id INT, v VECTOR(2));
INSERT INTO t_vcnt VALUES (1, '[1.0, 2.0]');
INSERT INTO t_vcnt VALUES (2, '[3.0, 4.0]');
INSERT INTO t_vcnt VALUES (3, NULL);
-- input:
SELECT COUNT(*), COUNT(v) FROM t_vcnt;
-- expected output:
3|2

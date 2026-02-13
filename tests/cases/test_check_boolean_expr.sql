-- CHECK constraint with boolean OR expression
-- setup:
CREATE TABLE t1 (id INT, status TEXT CHECK(status = 'active' OR status = 'archived'));
-- input:
INSERT INTO t1 (id, status) VALUES (1, 'active');
INSERT INTO t1 (id, status) VALUES (2, 'archived');
INSERT INTO t1 (id, status) VALUES (3, 'deleted');
-- expected output:
INSERT 0 1
INSERT 0 1
ERROR:  CHECK constraint violated for column 'status'
-- expected status: 1

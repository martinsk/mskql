-- CHECK constraint with function call
-- setup:
CREATE TABLE t1 (id INT, name TEXT CHECK(LENGTH(name) > 0));
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, '');
-- expected output:
INSERT 0 1
ERROR:  CHECK constraint violated for column 'name'
-- expected status: 1

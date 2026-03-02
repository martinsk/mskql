-- Bug: NOT NULL constraint is not enforced on UPDATE
-- UPDATE t SET v = NULL WHERE ... should fail when v has a NOT NULL constraint
-- mskql silently sets the value to NULL and returns UPDATE 1
-- setup:
CREATE TABLE t_nnu (id INT, v INT NOT NULL);
INSERT INTO t_nnu VALUES (1, 10);
-- input:
UPDATE t_nnu SET v = NULL WHERE id = 1;
-- expected output:
ERROR:  NOT NULL constraint violated for column 'v'
-- expected status: 1

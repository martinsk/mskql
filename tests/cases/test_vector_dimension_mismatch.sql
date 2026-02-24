-- vector: inserting wrong dimension should error
-- setup:
CREATE TABLE t_vdim (id INT, v VECTOR(3));
-- input:
INSERT INTO t_vdim VALUES (1, '[1.0, 2.0]');
-- expected status: error

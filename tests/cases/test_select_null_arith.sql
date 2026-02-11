-- Arithmetic with NULL should propagate NULL (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL);
-- input:
SELECT id, val + 10 FROM t1;
-- expected output:
1|
-- expected status: 0

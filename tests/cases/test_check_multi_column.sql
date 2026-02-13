-- CHECK constraint referencing multiple columns
-- setup:
CREATE TABLE t1 (id INT, lo INT, hi INT CHECK(hi > lo));
-- input:
INSERT INTO t1 (id, lo, hi) VALUES (1, 1, 10);
INSERT INTO t1 (id, lo, hi) VALUES (2, 10, 5);
-- expected output:
INSERT 0 1
ERROR:  CHECK constraint violated for column 'hi'
-- expected status: 1

-- chained :: casts: int -> text -> int
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (42);
-- input:
SELECT id::TEXT::INT FROM t1;
-- expected output:
42
-- expected status: 0

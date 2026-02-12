-- INSERT with fewer values than columns should pad with NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT, age INT);
INSERT INTO t1 VALUES (1, 'alice');
-- input:
SELECT id, name, age FROM t1;
-- expected output:
1|alice|
-- expected status: 0

-- MAX on text column - aggregate path uses cell_to_double which returns 0 for text
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'charlie'), (2, 'alice'), (3, 'bob');
-- input:
SELECT MAX(name) FROM t1;
-- expected output:
charlie
-- expected status: 0

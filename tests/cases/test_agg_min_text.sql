-- MIN/MAX on text columns uses cell_to_double which returns 0.0 for text
-- This means MIN/MAX on text columns likely returns wrong results
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'charlie'), (2, 'alice'), (3, 'bob');
-- input:
SELECT MIN(name) FROM t1;
-- expected output:
alice
-- expected status: 0

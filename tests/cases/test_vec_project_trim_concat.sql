-- Test vectorized TRIM and concat (||) projection
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, '  hello  ');
INSERT INTO t VALUES (2, 'world');
INSERT INTO t VALUES (3, '  test  ');
-- input:
SELECT TRIM(name), name || '!' FROM t ORDER BY id;
-- expected output:
hello|  hello  !
world|world!
test|  test  !

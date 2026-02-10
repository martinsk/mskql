-- SELECT with string concat expression and alias
-- setup:
CREATE TABLE t1 (id INT, first TEXT, last TEXT);
INSERT INTO t1 (id, first, last) VALUES (1, 'Alice', 'Smith'), (2, 'Bob', 'Jones');
-- input:
SELECT id, first || ' ' || last AS full_name FROM t1 ORDER BY id;
-- expected output:
1|Alice Smith
2|Bob Jones
-- expected status: 0

-- CONCAT function
-- setup:
CREATE TABLE t1 (id INT, first_name TEXT, last_name TEXT);
INSERT INTO t1 VALUES (1, 'Alice', 'Smith'), (2, 'Bob', NULL);
-- input:
SELECT id, CONCAT(first_name, ' ', last_name) FROM t1 ORDER BY id;
-- expected output:
1|Alice Smith
2|Bob 
-- expected status: 0

-- WHERE clause with string concatenation expression
-- setup:
CREATE TABLE t1 (first_name TEXT, last_name TEXT);
INSERT INTO t1 (first_name, last_name) VALUES ('Alice', 'Smith'), ('Bob', 'Jones');
-- input:
SELECT first_name FROM t1 WHERE first_name || ' ' || last_name = 'Alice Smith';
-- expected output:
Alice
-- expected status: 0

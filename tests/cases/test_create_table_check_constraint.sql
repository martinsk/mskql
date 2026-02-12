-- CREATE TABLE with CHECK constraint (parsed but may not be enforced)
-- setup:
CREATE TABLE t1 (id INT PRIMARY KEY, age INT CHECK (age > 0));
INSERT INTO t1 VALUES (1, 25);
-- input:
SELECT id, age FROM t1;
-- expected output:
1|25
-- expected status: 0

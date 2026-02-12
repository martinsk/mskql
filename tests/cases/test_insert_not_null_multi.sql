-- NOT NULL constraint should reject insert with NULL in constrained column
-- setup:
CREATE TABLE t1 (id INT NOT NULL, name TEXT NOT NULL);
-- input:
INSERT INTO t1 (id, name) VALUES (1, NULL);
-- expected output:
ERROR:  NOT NULL constraint violated for column 'name'
-- expected status: 1

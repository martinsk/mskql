-- INSERT with fewer columns than table has should fill defaults/NULLs
-- setup:
CREATE TABLE t1 (id INT, name TEXT DEFAULT 'unknown', active BOOLEAN DEFAULT TRUE);
-- input:
INSERT INTO t1 (id) VALUES (1);
SELECT * FROM t1;
-- expected output:
INSERT 0 1
1|unknown|t
-- expected status: 0

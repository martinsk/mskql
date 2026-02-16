-- bug: INSERT INTO ... DEFAULT VALUES fails with "expected VALUES or SELECT"
-- setup:
CREATE TABLE t_dv (id SERIAL, name TEXT DEFAULT 'unnamed', val INT DEFAULT 0);
-- input:
INSERT INTO t_dv DEFAULT VALUES;
SELECT id, name, val FROM t_dv;
-- expected output:
INSERT 0 1
1|unnamed|0

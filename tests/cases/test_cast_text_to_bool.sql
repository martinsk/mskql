-- CAST text to boolean
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, 'true');
INSERT INTO t1 (id, val) VALUES (2, 'false');
INSERT INTO t1 (id, val) VALUES (3, 'yes');
-- input:
SELECT val::BOOLEAN FROM t1;
-- expected output:
t
f
t
-- expected status: 0

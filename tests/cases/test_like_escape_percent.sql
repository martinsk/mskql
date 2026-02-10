-- LIKE with literal percent in data should match with %%
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, '50%'), (2, '100'), (3, '75% off');
-- input:
SELECT id, val FROM t1 WHERE val LIKE '%!%%' ORDER BY id;
-- expected output:
-- expected status: 0

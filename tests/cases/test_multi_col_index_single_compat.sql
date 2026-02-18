-- multi-column index: single-column index still works after refactor
-- setup:
CREATE TABLE t_mcis (id INT, val TEXT);
CREATE INDEX idx_mcis_id ON t_mcis (id);
INSERT INTO t_mcis VALUES (1, 'a');
INSERT INTO t_mcis VALUES (2, 'b');
INSERT INTO t_mcis VALUES (3, 'c');
-- input:
SELECT val FROM t_mcis WHERE id = 2;
-- expected output:
b

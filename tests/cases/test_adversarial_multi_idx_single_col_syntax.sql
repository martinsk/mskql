-- adversarial: single-column index via new multi-column parser path
-- setup:
CREATE TABLE t_amsc (id INT, val TEXT);
CREATE INDEX idx_amsc ON t_amsc (id);
INSERT INTO t_amsc VALUES (10, 'ten');
INSERT INTO t_amsc VALUES (20, 'twenty');
-- input:
SELECT val FROM t_amsc WHERE id = 10;
-- expected output:
ten

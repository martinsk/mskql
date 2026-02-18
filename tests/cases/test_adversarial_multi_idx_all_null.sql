-- adversarial: all indexed columns NULL, verify no crash
-- setup:
CREATE TABLE t_aman (a INT, b INT, val TEXT);
CREATE INDEX idx_aman ON t_aman (a, b);
INSERT INTO t_aman VALUES (NULL, NULL, 'both_null');
INSERT INTO t_aman VALUES (1, 2, 'normal');
-- input:
SELECT val FROM t_aman WHERE a = 1 AND b = 2;
-- expected output:
normal

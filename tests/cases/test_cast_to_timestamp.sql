-- CAST text to timestamp
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, '2024-03-15 10:30:00');
-- input:
SELECT val::TIMESTAMP FROM t1;
-- expected output:
2024-03-15 10:30:00
-- expected status: 0

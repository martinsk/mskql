-- bug: INTERVAL '...' literal syntax fails (only '...'::INTERVAL cast works)
-- setup:
CREATE TABLE t_interval_lit (id INT, d DATE);
INSERT INTO t_interval_lit VALUES (1, '2024-01-15');
-- input:
SELECT id, d + INTERVAL '1 month' AS next_month FROM t_interval_lit;
-- expected output:
1|2024-02-15
-- expected status: 0

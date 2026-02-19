-- bug: date - date should return integer (number of days) not interval string
-- setup:
CREATE TABLE t_date_diff (id INT, d1 DATE, d2 DATE);
INSERT INTO t_date_diff VALUES (1, '2024-01-01', '2024-01-31');
INSERT INTO t_date_diff VALUES (2, '2024-01-01', '2024-12-31');
-- input:
SELECT id, d2 - d1 AS days_diff FROM t_date_diff ORDER BY id;
-- expected output:
1|30
2|365
-- expected status: 0

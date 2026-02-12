-- LAG and LEAD window functions
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val, LEAD(val) OVER (ORDER BY id) AS next_val FROM t1 ORDER BY id;
-- expected output:
1|10||20
2|20|10|30
3|30|20|
-- expected status: 0

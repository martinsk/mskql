-- BUG: Window function with PARTITION BY and multiple ORDER BY columns fails with parse error
-- setup:
CREATE TABLE t (id INT, a TEXT, b TEXT, val INT);
INSERT INTO t VALUES (1, 'X', 'P', 10), (2, 'X', 'P', 20), (3, 'X', 'Q', 30), (4, 'Y', 'P', 40);
-- input:
SELECT id, a, b, val, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, val) AS rn FROM t ORDER BY id;
-- expected output:
1|X|P|10|1
2|X|P|20|2
3|X|Q|30|3
4|Y|P|40|1
-- expected status: 0

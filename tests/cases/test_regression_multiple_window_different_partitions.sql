-- bug: multiple window functions with different PARTITION BY use only the first partition
-- setup:
CREATE TABLE mw_part (id INT, grp1 TEXT, grp2 TEXT, val INT);
INSERT INTO mw_part VALUES (1, 'a', 'x', 10), (2, 'a', 'y', 20), (3, 'b', 'x', 30), (4, 'b', 'y', 40);
-- input:
SELECT id, SUM(val) OVER (PARTITION BY grp1) AS g1_sum, SUM(val) OVER (PARTITION BY grp2) AS g2_sum FROM mw_part ORDER BY id;
-- expected output:
1|30|40
2|30|60
3|70|40
4|70|60

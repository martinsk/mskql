-- bug: Window function query with LIMIT returns all rows instead of respecting LIMIT
-- setup:
CREATE TABLE t_wlim (id INT, grp TEXT, val INT);
INSERT INTO t_wlim VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'a',50),(6,'b',60),(7,'a',70),(8,'b',80);
-- input:
SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) as rn FROM t_wlim LIMIT 3;
-- expected output:
7|a|70|1
5|a|50|2
2|a|20|3

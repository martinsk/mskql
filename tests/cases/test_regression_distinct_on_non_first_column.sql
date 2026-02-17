-- DISTINCT ON should work when the column is not the first in the table
-- setup:
CREATE TABLE t (id INT, cat TEXT, val INT);
INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'a',5);
-- input:
SELECT DISTINCT ON (cat) cat, id, val FROM t ORDER BY cat, val DESC;
-- expected output:
a|2|20
b|4|40
-- expected status: 0

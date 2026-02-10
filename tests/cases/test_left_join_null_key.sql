-- LEFT JOIN where left table has NULL join key should not match any right rows
-- setup:
CREATE TABLE t1 (id INT, fk INT);
INSERT INTO t1 (id, fk) VALUES (1, 10), (2, NULL), (3, 20);
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (10, 'ten'), (20, 'twenty');
-- input:
SELECT t1.id, t2.name FROM t1 LEFT JOIN t2 ON t1.fk = t2.id ORDER BY t1.id;
-- expected output:
1|ten
2|
3|twenty
-- expected status: 0

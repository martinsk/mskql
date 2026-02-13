-- STRING_AGG with GROUP BY
-- setup:
CREATE TABLE t1 (id INT, category TEXT, name TEXT);
INSERT INTO t1 (id, category, name) VALUES (1, 'fruit', 'apple');
INSERT INTO t1 (id, category, name) VALUES (2, 'fruit', 'banana');
INSERT INTO t1 (id, category, name) VALUES (3, 'veggie', 'carrot');
INSERT INTO t1 (id, category, name) VALUES (4, 'veggie', 'daikon');
-- input:
SELECT category, STRING_AGG(name, ', ') FROM t1 GROUP BY category ORDER BY category;
-- expected output:
fruit|apple, banana
veggie|carrot, daikon
-- expected status: 0

-- Test CROSS JOIN with WHERE filter via plan executor
-- setup:
CREATE TABLE cjw_a (id INT, val TEXT);
CREATE TABLE cjw_b (id INT, label TEXT);
INSERT INTO cjw_a VALUES (1, 'x');
INSERT INTO cjw_a VALUES (2, 'y');
INSERT INTO cjw_b VALUES (10, 'alpha');
INSERT INTO cjw_b VALUES (20, 'beta');
-- input:
SELECT a.val, b.label FROM cjw_a a CROSS JOIN cjw_b b WHERE a.id = 1 ORDER BY b.label;
EXPLAIN SELECT a.val, b.label FROM cjw_a a CROSS JOIN cjw_b b WHERE a.id = 1 ORDER BY b.label
-- expected output:
x|alpha
x|beta
Project
  Sort
    Filter: (a.id = 1)
      Nested Loop
        Seq Scan on cjw_a
        Seq Scan on cjw_b

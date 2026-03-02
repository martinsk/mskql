-- Test CROSS JOIN via plan executor (nested loop)
-- setup:
CREATE TABLE cj_colors (id INT, color TEXT);
CREATE TABLE cj_sizes (id INT, size TEXT);
INSERT INTO cj_colors VALUES (1, 'red');
INSERT INTO cj_colors VALUES (2, 'blue');
INSERT INTO cj_sizes VALUES (1, 'S');
INSERT INTO cj_sizes VALUES (2, 'M');
INSERT INTO cj_sizes VALUES (3, 'L');
-- input:
SELECT c.color, s.size FROM cj_colors c CROSS JOIN cj_sizes s ORDER BY c.color, s.size;
EXPLAIN SELECT c.color, s.size FROM cj_colors c CROSS JOIN cj_sizes s ORDER BY c.color, s.size
-- expected output:
blue|L
blue|M
blue|S
red|L
red|M
red|S
Project
  Sort
    Nested Loop
      Seq Scan on cj_colors
      Seq Scan on cj_sizes

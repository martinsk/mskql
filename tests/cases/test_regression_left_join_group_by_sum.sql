-- bug: LEFT JOIN + GROUP BY + SUM on right-table column returns garbage value
-- setup:
CREATE TABLE lj_left (id INT, name TEXT);
CREATE TABLE lj_right (aid INT, val INT);
INSERT INTO lj_left VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO lj_right VALUES (1, 10), (1, 20), (2, 30);
-- input:
SELECT lj_left.name, SUM(lj_right.val) AS total FROM lj_left LEFT JOIN lj_right ON lj_left.id = lj_right.aid GROUP BY lj_left.name ORDER BY lj_left.name;
-- expected output:
a|30
b|30
c|

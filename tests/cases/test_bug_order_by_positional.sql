-- bug: ORDER BY with positional reference does not sort
-- setup:
CREATE TABLE ob_pos (id INT, name TEXT, val INT);
INSERT INTO ob_pos VALUES (1, 'c', 30), (2, 'a', 10), (3, 'b', 20);
-- input:
SELECT name, val FROM ob_pos ORDER BY 2;
-- expected output:
a|10
b|20
c|30

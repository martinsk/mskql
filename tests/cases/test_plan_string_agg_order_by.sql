-- Test STRING_AGG ORDER BY via plan executor
-- setup:
CREATE TABLE sa_test (id INT, grp TEXT, name TEXT);
INSERT INTO sa_test VALUES (1, 'A', 'cherry');
INSERT INTO sa_test VALUES (2, 'A', 'apple');
INSERT INTO sa_test VALUES (3, 'A', 'banana');
INSERT INTO sa_test VALUES (4, 'B', 'date');
INSERT INTO sa_test VALUES (5, 'B', 'elderberry');
INSERT INTO sa_test VALUES (6, 'B', 'fig');
-- input:
SELECT grp, STRING_AGG(name, ', ' ORDER BY name) AS names FROM sa_test GROUP BY grp ORDER BY grp;
-- expected output:
A|apple, banana, cherry
B|date, elderberry, fig

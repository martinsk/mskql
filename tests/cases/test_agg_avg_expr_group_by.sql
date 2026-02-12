-- AVG of expression with GROUP BY
-- setup:
CREATE TABLE scores (team TEXT, points INT, bonus INT);
INSERT INTO scores (team, points, bonus) VALUES ('X', 10, 5), ('X', 20, 10), ('Y', 30, 0), ('Y', 40, 5);
-- input:
SELECT team, AVG(points + bonus) FROM scores GROUP BY team ORDER BY team;
-- expected output:
X|22.5
Y|37.5
-- expected status: 0

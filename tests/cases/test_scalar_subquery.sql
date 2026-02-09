-- correlated scalar subquery in WHERE
-- setup:
CREATE TABLE scores (name TEXT, score INT);
INSERT INTO scores (name, score) VALUES ('alice', 90), ('bob', 60), ('carol', 80), ('dave', 50);
-- input:
SELECT name, score FROM scores WHERE score > (SELECT AVG(score) FROM scores) ORDER BY name;
-- expected output:
alice|90
carol|80
-- expected status: 0

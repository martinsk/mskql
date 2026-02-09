-- CASE WHEN with no ELSE should return NULL when no branch matches
-- setup:
CREATE TABLE scores (id INT, score INT);
INSERT INTO scores (id, score) VALUES (1, 90), (2, 40);
-- input:
SELECT id, CASE WHEN score >= 70 THEN 'pass' END FROM scores ORDER BY id;
-- expected output:
1|pass
2|
-- expected status: 0

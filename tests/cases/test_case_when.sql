-- CASE WHEN expression
-- setup:
CREATE TABLE "t1" (id INT, score INT);
INSERT INTO "t1" (id, score) VALUES (1, 90), (2, 60), (3, 40);
-- input:
SELECT id, CASE WHEN score >= 70 THEN 'pass' ELSE 'fail' END FROM "t1" ORDER BY id;
-- expected output:
1|pass
2|fail
3|fail
-- expected status: 0

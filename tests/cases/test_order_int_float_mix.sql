-- order by on column where comparison involves int vs float promotion
-- setup:
CREATE TABLE "t1" (id INT, score FLOAT);
INSERT INTO "t1" (id, score) VALUES (1, 3.5), (2, 1.2), (3, 2.8);
-- input:
SELECT id, score FROM "t1" WHERE score > 2 ORDER BY score;
-- expected output:
3|2.8
1|3.5
-- expected status: 0

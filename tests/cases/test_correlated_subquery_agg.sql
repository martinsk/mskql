-- correlated scalar subquery with aggregate (MAX) in SELECT list
-- setup:
CREATE TABLE csq_m (id INT, val INT);
CREATE TABLE csq_d (id INT, ref_id INT, score INT);
INSERT INTO csq_m VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO csq_d VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 3, 300), (5, 3, 150);
-- input:
SELECT csq_m.id, (SELECT MAX(csq_d.score) FROM csq_d WHERE csq_d.ref_id = csq_m.id) AS max_score FROM csq_m ORDER BY csq_m.id;
-- expected output:
1|200
2|50
3|300

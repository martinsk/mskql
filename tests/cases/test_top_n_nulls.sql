-- Top-N Sort: NULL handling
CREATE TABLE top_n_null (id INT, score INT);
INSERT INTO top_n_null VALUES (1, 50), (2, NULL), (3, 30), (4, NULL), (5, 10), (6, 70);
SELECT id, score FROM top_n_null ORDER BY score ASC LIMIT 3;
-- expect: 5|10, 3|30, 1|50
SELECT id, score FROM top_n_null ORDER BY score DESC LIMIT 3;
-- expect: 2|NULL or 4|NULL first (nulls first in DESC), then 6|70
SELECT id, score FROM top_n_null ORDER BY score ASC NULLS FIRST LIMIT 3;
-- expect: 2|NULL, 4|NULL, 5|10
DROP TABLE top_n_null;

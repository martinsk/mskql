-- Vectorized UPPER/LOWER
CREATE TABLE vec_ul (id INT, name TEXT);
INSERT INTO vec_ul VALUES (1, 'hello'), (2, 'World'), (3, NULL), (4, 'TEST');
SELECT UPPER(name) FROM vec_ul;
SELECT LOWER(name) FROM vec_ul;
SELECT UPPER(name), LOWER(name) FROM vec_ul;
DROP TABLE vec_ul;

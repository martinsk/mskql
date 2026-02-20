-- Vectorized LENGTH
CREATE TABLE vec_len (id INT, name TEXT);
INSERT INTO vec_len VALUES (1, 'hello'), (2, 'ab'), (3, NULL), (4, ''), (5, 'longstring');
SELECT LENGTH(name) FROM vec_len;
SELECT id, LENGTH(name) FROM vec_len;
DROP TABLE vec_len;

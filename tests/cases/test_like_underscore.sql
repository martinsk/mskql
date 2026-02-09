-- LIKE with underscore wildcard matching single character
-- setup:
CREATE TABLE words (id INT, word TEXT);
INSERT INTO words (id, word) VALUES (1, 'cat'), (2, 'car'), (3, 'card'), (4, 'ca');
-- input:
SELECT word FROM words WHERE word LIKE 'ca_' ORDER BY id;
-- expected output:
cat
car
-- expected status: 0

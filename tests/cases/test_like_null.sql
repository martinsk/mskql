-- LIKE on NULL column should not match
-- setup:
CREATE TABLE words (id INT, word TEXT);
INSERT INTO words (id, word) VALUES (1, 'hello'), (2, NULL), (3, 'world');
-- input:
SELECT id, word FROM words WHERE word LIKE '%' ORDER BY id;
-- expected output:
1|hello
3|world
-- expected status: 0

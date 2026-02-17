-- tutorial: sequences & enums - SERIAL auto-increment (sequences-enums.html)
-- setup:
CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO items (name) VALUES ('Alpha'), ('Beta'), ('Gamma');
-- input:
SELECT * FROM items ORDER BY id;
-- expected output:
1|Alpha
2|Beta
3|Gamma
-- expected status: 0

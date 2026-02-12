-- generate_series used to seed a table via INSERT...SELECT
-- setup:
CREATE TABLE nums (id INT);
INSERT INTO nums SELECT * FROM generate_series(1, 5);
-- input:
SELECT * FROM nums ORDER BY id;
-- expected output:
1
2
3
4
5
-- expected status: 0

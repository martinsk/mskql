-- INSERT ... SELECT with aggregate function
-- setup:
CREATE TABLE src (dept TEXT, salary INT);
INSERT INTO src (dept, salary) VALUES ('eng', 100), ('eng', 200), ('sales', 50);
CREATE TABLE dst (dept TEXT, total INT);
-- input:
INSERT INTO dst SELECT dept, SUM(salary) FROM src GROUP BY dept;
SELECT * FROM dst ORDER BY dept;
-- expected output:
INSERT 0 2
eng|300
sales|50
-- expected status: 0

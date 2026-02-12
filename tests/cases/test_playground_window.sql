-- playground example: window functions (RANK with PARTITION BY)
-- setup:
CREATE TABLE scores (student TEXT, subject TEXT, score INT);
INSERT INTO scores VALUES ('Alice', 'Math', 92), ('Alice', 'English', 88), ('Bob', 'Math', 85), ('Bob', 'English', 91), ('Charlie', 'Math', 96), ('Charlie', 'English', 79), ('Diana', 'Math', 88), ('Diana', 'English', 94);
-- input:
SELECT student, subject, score, RANK() OVER (PARTITION BY subject ORDER BY score DESC) AS rank FROM scores ORDER BY subject, rank;
-- expected output:
Diana|English|94|1
Bob|English|91|2
Alice|English|88|3
Charlie|English|79|4
Alice|Math|92|2
Diana|Math|88|3
Bob|Math|85|4
Charlie|Math|96|1
-- expected status: 0

-- tutorial: task tracker - UPDATE RETURNING (task-tracker.html)
-- setup:
CREATE TABLE projects (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE tasks (id SERIAL PRIMARY KEY, project_id INT NOT NULL REFERENCES projects(id), title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'todo', priority INT DEFAULT 0, created_at DATE DEFAULT '2025-01-15');
INSERT INTO projects (name) VALUES ('Website Redesign');
INSERT INTO tasks (project_id, title, status, priority, created_at) VALUES (1, 'Fix nav overflow', 'todo', 3, '2025-01-12');
UPDATE tasks SET status = 'done' WHERE title = 'Fix nav overflow';
-- input:
SELECT id, title, status FROM tasks WHERE title = 'Fix nav overflow';
-- expected output:
1|Fix nav overflow|done
-- expected status: 0

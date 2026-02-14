-- tutorial: task tracker - project summary GROUP BY on JOIN (task-tracker.html step 9)
-- setup:
CREATE TABLE projects (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE tasks (id SERIAL PRIMARY KEY, project_id INT NOT NULL REFERENCES projects(id), title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'todo', priority INT DEFAULT 0, created_at DATE DEFAULT '2025-01-15');
INSERT INTO projects (name) VALUES ('Website Redesign'), ('Mobile App'), ('API Platform');
INSERT INTO tasks (project_id, title, status, priority, created_at) VALUES (1, 'Update landing page', 'in_progress', 2, '2025-01-10'), (1, 'Fix nav overflow', 'todo', 3, '2025-01-12'), (1, 'Add dark mode', 'todo', 1, '2025-01-14'), (2, 'Login screen', 'done', 2, '2025-01-05'), (2, 'Push notifications', 'in_progress', 3, '2025-01-08'), (2, 'Offline sync', 'todo', 1, '2025-01-15'), (3, 'Rate limiting', 'todo', 3, '2025-01-11'), (3, 'OAuth2 integration', 'in_progress', 2, '2025-01-09'), (3, 'Batch endpoint', 'done', 1, '2025-01-06');
-- input:
SELECT p.name AS project, t.status, COUNT(*) AS tasks FROM tasks t JOIN projects p ON t.project_id = p.id GROUP BY p.name, t.status ORDER BY p.name, t.status;
-- expected output:
API Platform|done|1
API Platform|in_progress|1
API Platform|todo|1
Mobile App|done|1
Mobile App|in_progress|1
Mobile App|todo|1
Website Redesign|in_progress|1
Website Redesign|todo|2
-- expected status: 0

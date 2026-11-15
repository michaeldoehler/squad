-- Row-level security, as the owner of the tables. In PostgreSQL the
-- expected output must come from a role that is not a superuser and owns
-- the tables (run scripts/regress-expected.sh with PGUSER set to such a
-- role): superusers bypass every policy, owners only those of tables
-- without FORCE ROW LEVEL SECURITY. squad's default login is treated as
-- the owner.
CREATE TABLE docs (id int PRIMARY KEY, tenant text NOT NULL, body text);
INSERT INTO docs VALUES (1, 'acme', 'a1'), (2, 'globex', 'g1'), (3, 'acme', 'a2'), (4, 'initech', 'i1');
CREATE TABLE notes (doc_id int, note text);
INSERT INTO notes VALUES (1, 'n1'), (2, 'n2'), (3, 'n3'), (5, 'n5');
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname IN ('docs', 'notes') ORDER BY 1;
-- the owner bypasses the policies unless they are forced
SELECT count(*) FROM docs;
ALTER TABLE docs FORCE ROW LEVEL SECURITY;
SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'docs';
-- without a policy nothing is visible or writable
SELECT count(*) FROM docs;
INSERT INTO docs VALUES (5, 'acme', 'x');
UPDATE docs SET body = 'x';
DELETE FROM docs;
-- one policy for all commands
CREATE POLICY tenant_isolation ON docs USING (tenant = current_setting('app.tenant', true));
SELECT * FROM docs ORDER BY id;
SET app.tenant = 'acme';
SELECT * FROM docs ORDER BY id;
SELECT * FROM docs WHERE id = 2;
SELECT * FROM docs WHERE id IN (1, 2) ORDER BY id;
SELECT tenant, count(*) FROM docs GROUP BY tenant ORDER BY tenant;
SELECT min(id), max(id), count(body) FROM docs;
SET app.tenant = 'globex';
SELECT * FROM docs ORDER BY id;
RESET app.tenant;
SELECT * FROM docs ORDER BY id;
SELECT set_config('app.tenant', 'initech', false);
SELECT * FROM docs ORDER BY id;
BEGIN;
SET LOCAL app.tenant = 'acme';
SELECT id FROM docs ORDER BY id;
COMMIT;
SELECT id FROM docs ORDER BY id;
SET app.tenant = 'acme';
-- conditions of the query never see rows the policy hides
SELECT id FROM docs WHERE 1 / (CASE WHEN tenant = 'acme' THEN 1 ELSE 0 END) = 1 ORDER BY id;
-- joins and subqueries
SELECT d.id, n.note FROM docs d JOIN notes n ON n.doc_id = d.id ORDER BY d.id;
SELECT n.doc_id, d.body FROM notes n LEFT JOIN docs d ON d.id = n.doc_id ORDER BY n.doc_id;
SELECT note FROM notes WHERE doc_id IN (SELECT id FROM docs) ORDER BY note;
SELECT note FROM notes n WHERE NOT EXISTS (SELECT 1 FROM docs d WHERE d.id = n.doc_id) ORDER BY note;
SELECT (SELECT count(*) FROM docs) AS visible, (SELECT count(*) FROM notes) AS notes;
WITH t AS (SELECT * FROM docs) SELECT count(*) FROM t;
SELECT count(*) FROM (SELECT * FROM docs UNION ALL SELECT * FROM docs) u;
SELECT a.id, b.id FROM docs a, docs b WHERE a.id < b.id ORDER BY 1, 2;
-- views apply the policies of the tables they read
CREATE VIEW doc_bodies AS SELECT id, body FROM docs;
SELECT * FROM doc_bodies ORDER BY id;
SET app.tenant = 'globex';
SELECT * FROM doc_bodies ORDER BY id;
SET app.tenant = 'acme';
-- prepared statements read the setting when they run
PREPARE visible AS SELECT id FROM docs ORDER BY id;
PREPARE by_id(int) AS SELECT body FROM docs WHERE id = $1;
EXECUTE visible;
EXECUTE by_id(1);
EXECUTE by_id(2);
SET app.tenant = 'globex';
EXECUTE visible;
EXECUTE by_id(1);
EXECUTE by_id(2);
SET app.tenant = 'acme';
-- writes: USING decides which rows are changed, and new rows must pass it
INSERT INTO docs VALUES (5, 'acme', 'a3');
INSERT INTO docs VALUES (6, 'globex', 'g2');
INSERT INTO docs VALUES (6, NULL, 'g2');
INSERT INTO docs SELECT 7, 'acme', 'a4';
UPDATE docs SET body = body || '!' WHERE id IN (1, 2);
UPDATE docs SET tenant = 'globex' WHERE id = 1;
DELETE FROM docs WHERE id = 2;
DELETE FROM docs WHERE id = 7 RETURNING id, body;
UPDATE docs SET body = 'u' WHERE id = 5 RETURNING id, tenant, body;
INSERT INTO docs VALUES (1, 'acme', 'dup') ON CONFLICT DO NOTHING;
INSERT INTO docs VALUES (8, 'acme', 'a8') ON CONFLICT (id) DO NOTHING;
SET app.tenant = 'globex';
SELECT * FROM docs ORDER BY id;
SET app.tenant = 'acme';
SELECT * FROM docs ORDER BY id;
-- separate policies per command
DROP POLICY tenant_isolation ON docs;
CREATE POLICY sel ON docs FOR SELECT USING (tenant = current_setting('app.tenant') OR tenant = 'initech');
CREATE POLICY ins ON docs FOR INSERT WITH CHECK (tenant = current_setting('app.tenant'));
CREATE POLICY upd ON docs FOR UPDATE USING (tenant = current_setting('app.tenant')) WITH CHECK (body IS NOT NULL);
CREATE POLICY del ON docs FOR DELETE USING (id > 4);
SELECT * FROM docs ORDER BY id;
INSERT INTO docs VALUES (9, 'acme', 'a9');
INSERT INTO docs VALUES (10, 'initech', 'i2');
UPDATE docs SET body = 'upd' WHERE tenant = 'initech';
UPDATE docs SET body = NULL WHERE id = 1;
UPDATE docs SET body = 'ok' WHERE id = 1;
UPDATE docs SET tenant = 'globex' WHERE id = 1;
UPDATE docs SET body = 'moved' WHERE id = 1 RETURNING id;
SET app.tenant = 'acme';
DELETE FROM docs WHERE id < 4;
DELETE FROM docs WHERE id = 9;
SELECT * FROM docs ORDER BY id;
-- INSERT ... RETURNING also needs the SELECT policies
DROP POLICY ins ON docs;
CREATE POLICY ins ON docs FOR INSERT WITH CHECK (true);
INSERT INTO docs VALUES (11, 'globex', 'g3');
INSERT INTO docs VALUES (12, 'globex', 'g4') RETURNING id;
INSERT INTO docs VALUES (12, 'acme', 'a12') RETURNING id;
-- SELECT ... FOR UPDATE needs the UPDATE policies
SELECT id FROM docs ORDER BY id FOR UPDATE;
-- restrictive policies narrow the permissive ones
CREATE POLICY no_drafts ON docs AS RESTRICTIVE USING (body NOT LIKE 'draft%');
INSERT INTO docs VALUES (13, 'acme', 'draft 1');
INSERT INTO docs VALUES (13, 'acme', 'final 1');
UPDATE docs SET body = 'draft 2' WHERE id = 1;
SELECT * FROM docs ORDER BY id;
-- restrictive policies alone allow nothing
DROP POLICY sel ON docs;
SELECT count(*) FROM docs;
CREATE POLICY sel ON docs FOR SELECT USING (true);
SELECT count(*) FROM docs;
-- policies for other roles do not apply; CURRENT_USER does
DROP POLICY sel ON docs;
CREATE POLICY sel ON docs FOR SELECT TO pg_monitor USING (true);
SELECT count(*) FROM docs;
ALTER POLICY sel ON docs TO CURRENT_USER;
SELECT count(*) FROM docs;
ALTER POLICY sel ON docs TO PUBLIC;
ALTER POLICY sel ON docs USING (id < 5);
SELECT id FROM docs ORDER BY id;
ALTER POLICY sel ON docs RENAME TO sel_low;
ALTER POLICY sel_low ON docs RENAME TO ins;
ALTER POLICY nope ON docs RENAME TO other;
ALTER POLICY nope ON docs USING (true);
SELECT tablename, policyname, permissive, roles, cmd FROM pg_policies ORDER BY policyname;
SELECT polname, polcmd, polpermissive FROM pg_policy ORDER BY polname;
-- errors in policy definitions
CREATE POLICY ins ON docs USING (true);
CREATE POLICY p ON docs FOR SELECT USING (true) WITH CHECK (true);
CREATE POLICY p ON docs FOR DELETE WITH CHECK (true);
CREATE POLICY p ON docs FOR INSERT USING (true);
CREATE POLICY p ON docs USING (id);
CREATE POLICY p ON docs USING (count(*) > 0);
CREATE POLICY p ON docs USING (nope = 1);
CREATE POLICY p ON nope USING (true);
CREATE POLICY p ON doc_bodies USING (true);
CREATE POLICY p ON pg_class USING (true);
ALTER POLICY upd ON docs FOR SELECT;
ALTER POLICY del ON docs WITH CHECK (true);
DROP POLICY nope ON docs;
DROP POLICY IF EXISTS nope ON docs;
DROP POLICY IF EXISTS nope ON no_table;
DROP POLICY nope ON no_table;
-- columns used by policies
ALTER TABLE docs DROP COLUMN body;
ALTER TABLE docs ALTER COLUMN body TYPE varchar(100);
-- row_security = off fails instead of filtering
SET row_security = off;
SELECT count(*) FROM docs;
SELECT count(*) FROM notes;
RESET row_security;
SELECT count(*) FROM docs;
-- policies that refer to tables with policies of their own
CREATE TABLE a (x int);
CREATE TABLE b (x int);
INSERT INTO a VALUES (1), (2);
INSERT INTO b VALUES (1);
ALTER TABLE a ENABLE ROW LEVEL SECURITY;
ALTER TABLE a FORCE ROW LEVEL SECURITY;
CREATE POLICY pa ON a USING (x IN (SELECT x FROM b));
SELECT * FROM a;
ALTER TABLE b ENABLE ROW LEVEL SECURITY;
ALTER TABLE b FORCE ROW LEVEL SECURITY;
CREATE POLICY pb ON b USING (x IN (SELECT x FROM a));
SELECT * FROM a;
DROP POLICY pb ON b;
CREATE POLICY pb ON b USING (true);
SELECT * FROM a;
-- transactional DDL
BEGIN;
DROP POLICY pa ON a;
SELECT count(*) FROM a;
ROLLBACK;
SELECT count(*) FROM a;
BEGIN;
ALTER TABLE a DISABLE ROW LEVEL SECURITY;
SELECT count(*) FROM a;
ROLLBACK;
SELECT count(*) FROM a;
-- without FORCE the owner sees everything again; disabling keeps the policies
ALTER TABLE docs NO FORCE ROW LEVEL SECURITY;
SELECT count(*) FROM docs;
ALTER TABLE docs FORCE ROW LEVEL SECURITY;
ALTER TABLE docs DISABLE ROW LEVEL SECURITY;
SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'docs';
SELECT count(*) FROM docs;
SELECT count(*) FROM pg_policies WHERE tablename = 'docs';
DROP TABLE docs CASCADE;
SELECT count(*) FROM pg_policies WHERE tablename = 'docs';

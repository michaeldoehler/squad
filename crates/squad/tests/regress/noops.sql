-- Statements that squad accepts without effect, or implements only in
-- part, where the visible result matches PostgreSQL. squad adds a
-- WARNING or NOTICE to some of them; the comparison ignores notices.
CREATE TABLE noop_t (id int PRIMARY KEY, v int);
INSERT INTO noop_t VALUES (1, 10), (2, 20);
-- Privileges and comments
GRANT SELECT ON noop_t TO PUBLIC;
GRANT SELECT, INSERT ON TABLE noop_t TO PUBLIC;
REVOKE ALL ON noop_t FROM PUBLIC;
GRANT SELECT ON noop_missing TO PUBLIC;
REVOKE SELECT ON noop_missing FROM PUBLIC;
COMMENT ON TABLE noop_t IS 'a table';
ALTER TABLE noop_t OWNER TO CURRENT_USER;
SELECT count(*) FROM noop_t;
-- LOCK TABLE needs a transaction block
LOCK TABLE noop_t IN ACCESS SHARE MODE;
LOCK TABLE noop_t IN ROW EXCLUSIVE MODE;
BEGIN;
LOCK TABLE noop_t IN ACCESS SHARE MODE;
LOCK TABLE noop_t IN ROW SHARE MODE;
LOCK noop_t IN ROW EXCLUSIVE MODE;
LOCK TABLE noop_t IN EXCLUSIVE MODE NOWAIT;
UPDATE noop_t SET v = v + 1 WHERE id = 1;
SELECT * FROM noop_t ORDER BY id;
COMMIT;
BEGIN;
LOCK TABLE noop_missing IN ROW SHARE MODE;
ROLLBACK;
BEGIN READ ONLY;
LOCK TABLE noop_t IN ACCESS SHARE MODE;
LOCK TABLE noop_t IN ROW EXCLUSIVE MODE;
LOCK TABLE noop_t IN EXCLUSIVE MODE;
ROLLBACK;
-- Constraints are always checked immediately
SET CONSTRAINTS ALL IMMEDIATE;
REINDEX TABLE noop_t;
-- statement_timeout
SHOW statement_timeout;
SET statement_timeout = 100;
SHOW statement_timeout;
SELECT count(*) FROM noop_t;
RESET statement_timeout;
SHOW statement_timeout;

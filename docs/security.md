# Logins, read-only access and row-level security

squad has no SQL roles and no privileges. What it has is the login a
session runs as, two attributes per login, and PostgreSQL's row-level
security: `ALTER TABLE … ENABLE ROW LEVEL SECURITY` and `CREATE POLICY`
with PostgreSQL 16's semantics, as far as they can be expressed without
roles. This page describes the model, how to configure it and where it
differs from PostgreSQL.

The typical use is a multi-tenant table read by a tool that connects
with its own login and says which tenant it works for, per query:

```sql
-- run once, by the application (the owner)
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON events
    USING (tenant_id = current_setting('app.current_tenant', true));

-- the tool, logged in as a login without bypass_rls
SET app.current_tenant = 'acme';
SELECT kind, count(*) FROM events GROUP BY kind;   -- acme's rows only
```

Without `app.current_tenant`, `current_setting(…, true)` is NULL, the
condition is never true, and the tool sees no rows.

## Logins

Every session runs as a login:

- **Server:** the user name of the connection, after authentication
  ([server](server.md#authentication)).
- **Embedded:** `Database::connect` runs as the login `squad`;
  `Database::connect_as(Login { user, bypass_rls, read_only })` as any
  other; `Session::set_login` changes the login of a raw session.

`current_user`, `session_user`, `user` and `current_role` return the
login name. `SET ROLE` and `SET SESSION AUTHORIZATION` accept only the
login's own name, `NONE` and `DEFAULT`; any other name fails with
`0A000`, because there is no role to switch to. `CREATE ROLE`, `GRANT`
and `REVOKE` are still accepted with a warning and have no effect (see
[compatibility](compatibility.md#accepted-without-effect)).

A login has two attributes:

| Attribute | Default | Meaning |
| --- | --- | --- |
| `bypass_rls` | true | The login is treated as the owner of every table: row-level security policies apply to it only on tables with `FORCE ROW LEVEL SECURITY`. Without it, the login is subject to the policies of every table that has row-level security enabled. |
| `read_only` | false | Every transaction of the login is read-only. |

The defaults are what every login was before these attributes existed,
so a configuration that does not mention them behaves as before. On the
server they are set in the users file, with command-line options or in
`ServerConfig::logins` ([server](server.md#login-attributes)).

## Read-only logins

For a read-only login, every transaction is read-only, as with
PostgreSQL's `default_transaction_read_only`, except that it cannot be
turned off:

- `INSERT`, `UPDATE`, `DELETE`, `SELECT … FOR UPDATE/SHARE`, `COPY …
  FROM`, `nextval`, `setval` and every DDL statement fail with SQLSTATE
  `25006` ("cannot execute … in a read-only transaction").
- `BEGIN READ WRITE`, `SET TRANSACTION READ WRITE` and `SET
  transaction_read_only = off` fail with `25006`. `SET
  default_transaction_read_only = off` is accepted and changes nothing.
- Queries, `SET`, `SHOW`, `PREPARE`/`EXECUTE`, advisory locks and
  `VACUUM` work. `SHOW transaction_read_only` and `SHOW
  default_transaction_read_only` return `on`.

`default_transaction_read_only` and `transaction_read_only` also work for
every other session as in PostgreSQL: they make transactions read-only
by default, and a transaction can still ask for `READ WRITE`.

## Custom settings

Policies usually read the tenant from a custom setting: any name with a
dot, such as `app.current_tenant`. They behave as in PostgreSQL:

| Statement | Effect |
| --- | --- |
| `SET app.current_tenant = 'acme'`, `SET SESSION …` | for the session; undone if the surrounding transaction rolls back |
| `SET LOCAL app.current_tenant = 'acme'` | until the end of the transaction (commit or rollback); outside a transaction block a warning and no effect |
| `SELECT set_config('app.current_tenant', 'acme', false)` | like `SET`; returns the new value. With `true` like `SET LOCAL`; outside a transaction block it lasts until the end of the statement. A NULL value resets the setting. |
| `RESET app.current_tenant`, `SET … TO DEFAULT` | the setting becomes empty |
| `SHOW app.current_tenant`, `current_setting('app.current_tenant')` | the value; `42704` ("unrecognized configuration parameter") if the setting was never set in the session |
| `current_setting('app.current_tenant', true)` | NULL instead of the error |
| `ROLLBACK TO SAVEPOINT` | undoes the setting changes made after the savepoint |
| `RESET ALL`, `DISCARD ALL` | every custom setting becomes empty |

Once set, a custom setting stays known for the rest of the session: after
`RESET`, a rolled-back `SET` or the end of a `SET LOCAL` it is empty
(`''`), not unknown. A policy that must treat "not set" and "reset"
alike compares with `NULLIF(current_setting('app.current_tenant', true),
'')`, or simply relies on no tenant being called `''`.

Settings can also be given at login (`options=-c app.current_tenant=acme`
in libpq). A login is not prevented from setting any tenant it likes:
the setting chooses among the rows the policies allow, it is not an
authentication. Give each tenant a login of its own and policies `TO`
that login if tenants must be kept from each other's data even when the
client misbehaves.

## Row-level security

```sql
ALTER TABLE t ENABLE ROW LEVEL SECURITY;     -- pg_class.relrowsecurity
ALTER TABLE t FORCE ROW LEVEL SECURITY;      -- pg_class.relforcerowsecurity
ALTER TABLE t NO FORCE ROW LEVEL SECURITY;
ALTER TABLE t DISABLE ROW LEVEL SECURITY;    -- the policies are kept

CREATE POLICY name ON t
    [ AS { PERMISSIVE | RESTRICTIVE } ]
    [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
    [ TO { login_name | PUBLIC | CURRENT_USER | SESSION_USER | CURRENT_ROLE } [, …] ]
    [ USING ( condition ) ]
    [ WITH CHECK ( condition ) ];
ALTER POLICY name ON t RENAME TO new_name;
ALTER POLICY name ON t [ TO … ] [ USING ( … ) ] [ WITH CHECK ( … ) ];
DROP POLICY [ IF EXISTS ] name ON t;
```

Ordinary and columnar tables have the same row-level security. All of it
is transactional DDL and stored in the catalog.

### Who is subject to the policies

| Login | Table without `FORCE` | Table with `FORCE` |
| --- | --- | --- |
| `bypass_rls` (the default, and the embedded API's `squad`) | not subject | subject |
| without `bypass_rls` | subject | subject |

A login with `bypass_rls` corresponds to a table's owner in PostgreSQL. A
table with `FORCE ROW LEVEL SECURITY` is how such a login tests its own
policies. There is no counterpart of a PostgreSQL superuser or a
`BYPASSRLS` role, which ignore `FORCE` too.

A login without `bypass_rls` is not an owner: it cannot enable, disable
or force row-level security on any table, nor create, change or drop
policies, and it cannot `ALTER`, `DROP`, `TRUNCATE`, rename or index a
table that has row-level security enabled (SQLSTATE `42501`, "must be
owner of table …"). On tables without row-level security it can do
everything, as before. Its `COPY … FROM` into a table whose policies
apply to it fails with `0A000`, as in PostgreSQL; `INSERT` works.

`TO` names logins. `PUBLIC`, or no `TO`, means every login subject to
the policies. `CURRENT_USER`, `SESSION_USER` and `CURRENT_ROLE` are
replaced by the name of the login that creates or alters the policy.
Names are not checked against the configured logins, since there are no
roles in SQL.

### Which rows

When the policies of a table apply to a statement, PostgreSQL's rules
decide which rows it sees and which it may write:

| Command | Existing rows must pass (`USING`) | New rows must pass (`WITH CHECK`) |
| --- | --- | --- |
| `SELECT` | SELECT policies | — |
| `SELECT … FOR UPDATE/SHARE` | SELECT and UPDATE policies | — |
| `INSERT` | — | INSERT policies; with `RETURNING` also the `USING` of the SELECT policies |
| `UPDATE` | UPDATE policies; SELECT policies too when the statement reads the table's columns (`WHERE`, `SET x = x + 1`, `RETURNING`) | UPDATE policies; SELECT policies' `USING` when it reads the columns |
| `DELETE` | DELETE policies; SELECT policies too when `WHERE` or `RETURNING` read the columns | — |

- "SELECT policies" are the policies `FOR SELECT` and `FOR ALL` that apply
  to the login; likewise for the other commands.
- The permissive policies are combined with `OR`, the restrictive ones
  with `AND`, and a row must pass both parts. Without a permissive policy
  for the command, no row passes, also when restrictive ones exist. A
  table with row-level security and no policy shows nothing and accepts
  nothing.
- A policy without `WITH CHECK` uses its `USING` condition for new rows.
- Rows that fail `USING` are silently left out. A new row that fails
  `WITH CHECK` fails the statement with `42501`, "new row violates
  row-level security policy for table "t"" (with the policy's name for a
  restrictive policy). A NULL result fails both.
- The policies of every table a statement reads apply: in joins,
  subqueries, CTEs, views, `INSERT … SELECT`, `CREATE TABLE … AS` and
  `COPY … TO`. Prepared statements read the settings when they run, so
  one prepared statement serves every tenant.
- A policy may read other tables, whose policies then apply as well; a
  cycle fails with `42P17` ("infinite recursion detected in policy").
- `SET row_security = off` makes a statement that policies would filter
  fail with `42501` instead, as in PostgreSQL (`pg_dump` sets it). For
  logins the policies do not apply to, it changes nothing.

### How policies are applied

The planner adds a table's policies where the table is read: the scan
of the table is filtered by the combined `USING` condition, and the
plans of `INSERT` and `UPDATE` carry the `WITH CHECK` conditions, which
are checked before `NOT NULL` and `CHECK` constraints. The statement's
own conditions are evaluated after the policy's, row by row, so a
condition such as `1 / (amount - 5) = 0` never sees, and never fails
on, a row the policy hides. Plans depend on the login, and the plan
cache keeps them apart.

`current_setting` is evaluated once per statement, not per row, like a
stable function in PostgreSQL. A condition such as `tenant_id =
current_setting('app.current_tenant', true)` therefore works like a
comparison with a constant: on an ordinary table it can use an index or
the primary key on `tenant_id`, and on a columnar table the column
engine evaluates it, skips the segments and row groups of other tenants
by their statistics, and aggregates the rest vectorized. The policy
costs no more than writing the condition into the query. Conditions the
column engine cannot express fall back to row-by-row evaluation, as
for any query.

### Views

A view applies the policies of the tables it reads **for the login that
runs the query**. In PostgreSQL a view runs with its owner's rights
unless it is created `WITH (security_invoker = true)`; since the owner
is usually exempt from its own policies, a PostgreSQL view by default
shows every row. squad has no view owners, and always behaves like
`security_invoker = true`: a view never shows a login more than the
login could read from the tables directly.

### Catalogs

`pg_class.relrowsecurity` and `relforcerowsecurity`, `pg_policy` and
the `pg_policies` view show the settings and policies; psql's `\d table`
lists them. `pg_roles` lists the session's login and every name a policy
applies to. The policy conditions are shown as squad stores them, which
can differ in spelling from PostgreSQL's `pg_get_expr` (casts PostgreSQL
adds, such as `'acme'::text`, are not shown).

## Differences from PostgreSQL

- Logins are not roles: no role membership, no superuser or `BYPASSRLS`
  role that ignores `FORCE ROW LEVEL SECURITY`, no `GRANT`. A login with
  `bypass_rls` is treated as the owner of every table.
- Views always apply the policies for the querying login (see above).
- `INSERT … ON CONFLICT DO UPDATE` on a table whose policies apply to the
  statement fails with `0A000`. `ON CONFLICT DO NOTHING` works.
- `SELECT … FOR UPDATE/SHARE` on a subquery over such a table fails with
  `0A000`; on the table itself it works.
- Renaming a column that a policy condition mentions, or a table whose
  name a policy condition mentions, fails with `0A000` (PostgreSQL
  updates the policy). Changing the type of such a column fails with
  `0A000`, and dropping it fails with `2BP01` unless `CASCADE` drops the
  policy, as in PostgreSQL. squad decides by the names in the stored
  condition, so a name that only appears inside a string literal also
  counts.
- Policy names given in `TO` are not checked.
- As in PostgreSQL, policies do not hide rows from constraint checks: a
  unique or foreign key violation can reveal that a hidden row exists.
  Foreign key actions (`ON DELETE CASCADE`, …) change referencing rows
  regardless of their policies.
- Read-only logins have no PostgreSQL counterpart; PostgreSQL's
  `default_transaction_read_only` can be overridden by the client.

-- int2vector and oidvector, as pg_index uses them.
CREATE TABLE vx (id int PRIMARY KEY, a int, b text, c varchar(10), d bigint, UNIQUE (b), CONSTRAINT vx_ac UNIQUE (a, c));
CREATE INDEX vx_dca ON vx (d, c, a);
-- text form, subscripts from 0, casts to text
SELECT i.relname, ix.indkey, ix.indoption, ix.indclass, ix.indcollation, pg_typeof(ix.indkey), pg_typeof(ix.indclass) FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'vx'::regclass ORDER BY 1;
SELECT i.relname, ix.indkey::varchar, ix.indoption::varchar, ix.indkey::text || '!', ix.indkey[0], ix.indkey[1], ix.indkey[2], ix.indkey[3], ix.indkey[-1] FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'vx'::regclass ORDER BY 1;
SELECT i.relname, array_lower(ix.indkey, 1), array_upper(ix.indkey, 1), array_length(ix.indkey, 1), array_dims(ix.indkey), cardinality(ix.indkey), array_ndims(ix.indkey) FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'vx'::regclass ORDER BY 1;
SELECT i.relname, array_position(ix.indkey, 1::int2), array_position(ix.indkey, 2::int2), array_position(ix.indkey, 4), array_position(ix.indkey, 2::int2, 1), array_positions(ix.indkey, 4::int2), ix.indkey[1:2], ix.indkey[:0], array_to_string(ix.indkey, ',') FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'vx'::regclass ORDER BY 1;
SELECT i.relname, generate_subscripts(ix.indkey, 1) AS ord, unnest(ix.indkey) AS attnum FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'vx'::regclass ORDER BY 1, 2;
SELECT i.relname, u.attnum, u.ord, o.opcname, o.opcdefault FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid, unnest(ix.indkey, ix.indclass) WITH ORDINALITY u(attnum, opclass, ord) LEFT JOIN pg_opclass o ON o.oid = u.opclass WHERE ix.indrelid = 'vx'::regclass ORDER BY 1, 3;
SELECT i.relname, ix.indkey = '2 4', ix.indkey <> '2 4', 3 = ANY (ix.indkey), 3 = ALL (ix.indkey), to_json(ix.indkey) FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'vx'::regclass ORDER BY 1;
-- the index introspection query of SQLAlchemy 1.4 (used by Apache Superset)
SELECT i.relname as relname, ix.indisunique, ix.indexprs, a.attname, a.attnum, c.contype, ix.indkey::varchar, ix.indoption::varchar, i.reloptions, am.amname, pg_get_expr(ix.indpred, ix.indrelid), ix.indnkeyatts as indnkeyatts FROM pg_class t join pg_index ix on t.oid = ix.indrelid join pg_class i on i.oid = ix.indexrelid left outer join pg_attribute a on t.oid = a.attrelid and a.attnum = ANY(ix.indkey) left outer join pg_constraint c on (ix.indrelid = c.conrelid and ix.indexrelid = c.conindid and c.contype in ('p', 'u', 'x')) left outer join pg_am am on i.relam = am.oid WHERE t.relkind IN ('r', 'v', 'f', 'm', 'p') and t.oid = 'vx'::regclass and ix.indisprimary = 'f' ORDER BY t.relname, i.relname, a.attnum;
-- literals
SELECT '3 1  4'::int2vector, ' 7 '::int2vector, ''::int2vector, '1 2'::oidvector, pg_typeof('1'::oidvector);
SELECT ('3 1 4'::int2vector)[0], ('3 1 4'::int2vector)[2], ('3 1 4'::int2vector)[3], ('3 1 4'::int2vector)[1:];
SELECT array_dims(''::int2vector), array_length(''::int2vector, 1), array_upper(''::int2vector, 1), array_lower(''::int2vector, 1), cardinality(''::int2vector);
SELECT '1 2'::int2vector = '1 2'::int2vector, '1 2'::int2vector < '1 3'::int2vector, '1 2'::int2vector::varchar(2) AS v;
SELECT unnest('3 1 4'::int2vector), generate_subscripts('5 6 7'::oidvector, 1);
SELECT x FROM (VALUES ('1 2'::int2vector), ('1 2'::int2vector), ('3'::int2vector)) v(x) GROUP BY x ORDER BY x;
SELECT '{1,2}'::int2vector;
SELECT '40000'::int2vector;
SELECT 'x'::oidvector;
SELECT ARRAY[1, 2]::int2[]::int2vector;
SELECT format_type('int2vector'::regtype, -1), format_type('oidvector'::regtype, -1), format_type('_int2vector'::regtype, -1);

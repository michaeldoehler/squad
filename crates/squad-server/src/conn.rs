//! One client connection: startup, authentication and the query protocols.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use squad::copy::{self, CopyDecoder, CopyRequest};
use squad::session::Prepared;
use squad::value::{decode_binary, encode_binary, format_value, parse_value};
use squad::{sqlstate, Error, FnCtx, OutputColumn, QueryResult, Session, TxnStatus, Type, Value};

use crate::proto::{Io, Reader};
use crate::scram;
use crate::{AuthData, Inner};

const SSL_REQUEST: u32 = 80877103;
const GSSENC_REQUEST: u32 = 80877104;
const CANCEL_REQUEST: u32 = 80877102;
/// Rows per INSERT while loading COPY data.
const COPY_BATCH: usize = 64;
/// Output is written out once this much has accumulated.
const FLUSH_THRESHOLD: usize = 64 * 1024;

/// Why handling a message failed.
enum Fail {
    /// A database error: reported to the client, the connection lives on.
    Db(Error),
    /// The connection is unusable.
    Io(io::Error),
}

impl From<Error> for Fail {
    fn from(e: Error) -> Fail {
        Fail::Db(e)
    }
}

impl From<io::Error> for Fail {
    fn from(e: io::Error) -> Fail {
        Fail::Io(e)
    }
}

type Step<T = ()> = Result<T, Fail>;

#[derive(Clone)]
enum StmtKind {
    Sql(Arc<Prepared>),
    Copy(Box<CopyRequest>),
}

struct Stmt {
    kind: StmtKind,
}

struct Portal {
    kind: StmtKind,
    params: Vec<Value>,
    formats: Vec<i16>,
    /// Result once executed, and how many rows were already sent.
    result: Option<(QueryResult, usize)>,
}

pub(crate) struct Conn {
    io: Io,
    session: Session,
    inner: Arc<Inner>,
    stmts: HashMap<String, Stmt>,
    portals: HashMap<String, Portal>,
    reported_tz: String,
    reported_app: String,
}

fn protocol_violation(msg: impl Into<String>) -> Error {
    Error::new(sqlstate::PROTOCOL_VIOLATION, msg)
}

fn contains_copy(sql: &str) -> bool {
    sql.as_bytes().windows(4).any(|w| w.eq_ignore_ascii_case(b"copy"))
}

fn type_len(ty: &Type) -> i16 {
    match ty {
        Type::Bool => 1,
        Type::Int2 => 2,
        Type::Int4 | Type::Float4 | Type::Date => 4,
        Type::Int8 | Type::Float8 | Type::Time | Type::Timestamp | Type::TimestampTz => 8,
        Type::Interval | Type::Uuid => 16,
        _ => -1,
    }
}

fn format_at(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

fn row_description(io: &mut Io, columns: &[OutputColumn], formats: &[i16]) {
    let m = io.begin(b'T');
    io.i16(columns.len() as i16);
    for (i, c) in columns.iter().enumerate() {
        io.cstr(&c.name);
        io.i32(0); // table OID
        io.i16(0); // column number
        io.i32(c.ty.oid() as i32);
        io.i16(type_len(&c.ty));
        io.i32(-1); // type modifier
        io.i16(format_at(formats, i));
    }
    io.end(m);
}

fn data_row(io: &mut Io, columns: &[OutputColumn], row: &[Value], formats: &[i16], ctx: &FnCtx) -> Result<(), Error> {
    let m = io.begin(b'D');
    io.i16(row.len() as i16);
    for (i, v) in row.iter().enumerate() {
        if v.is_null() {
            io.i32(-1);
            continue;
        }
        let at = io.out.len();
        io.i32(0);
        if format_at(formats, i) == 1 {
            let ty = columns.get(i).map_or(Type::Text, |c| c.ty.clone());
            encode_binary(v, &ty, &mut io.out)?;
        } else {
            io.out.extend_from_slice(format_value(v, ctx).as_bytes());
        }
        let len = (io.out.len() - at - 4) as i32;
        io.out[at..at + 4].copy_from_slice(&len.to_be_bytes());
    }
    io.end(m);
    Ok(())
}

fn command_complete(io: &mut Io, tag: &str) {
    if tag.is_empty() {
        io.message(b'I', &[]);
    } else {
        let m = io.begin(b'C');
        io.cstr(tag);
        io.end(m);
    }
}

fn error_response(io: &mut Io, severity: &str, e: &Error) {
    let m = io.begin(b'E');
    for (code, value) in [(b'S', severity), (b'V', severity), (b'C', e.code()), (b'M', e.message())] {
        io.out.push(code);
        io.cstr(value);
    }
    if let Some(d) = e.detail() {
        io.out.push(b'D');
        io.cstr(d);
    }
    if let Some(h) = e.hint() {
        io.out.push(b'H');
        io.cstr(h);
    }
    if let Some(p) = e.position() {
        io.out.push(b'P');
        io.cstr(&p.to_string());
    }
    io.out.push(0);
    io.end(m);
}

fn io_as_db(e: io::Error) -> Error {
    Error::new("08006", format!("connection failure: {e}"))
}

impl Conn {
    pub(crate) fn new(io: Io, session: Session, inner: Arc<Inner>) -> Conn {
        Conn {
            io,
            session,
            inner,
            stmts: HashMap::new(),
            portals: HashMap::new(),
            reported_tz: String::new(),
            reported_app: String::new(),
        }
    }

    fn fn_ctx(&self) -> FnCtx {
        FnCtx { tz: self.session.state().tz, ..FnCtx::default() }
    }

    /// Reports an unrecoverable error and gives up the connection.
    fn fatal(&mut self, e: Error) -> io::Result<()> {
        error_response(&mut self.io, "FATAL", &e);
        self.io.flush()
    }

    fn parameter_status(&mut self, name: &str, value: &str) {
        let m = self.io.begin(b'S');
        self.io.cstr(name);
        self.io.cstr(value);
        self.io.end(m);
    }

    fn auth_message(&mut self, code: i32, data: &[u8]) {
        let m = self.io.begin(b'R');
        self.io.i32(code);
        self.io.out.extend_from_slice(data);
        self.io.end(m);
    }

    /// Reads a password ('p') message during authentication.
    fn read_password_message(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self.io.read_message()? {
            Some((b'p', body)) => Ok(Some(body)),
            _ => Ok(None),
        }
    }

    fn authenticate(&mut self, user: &str) -> io::Result<bool> {
        let failed = |user: &str| {
            Error::new(sqlstate::INVALID_PASSWORD, format!("password authentication failed for user \"{user}\""))
        };
        let inner = self.inner.clone();
        match &inner.auth {
            AuthData::Trust => {}
            AuthData::Cleartext(users) => {
                self.auth_message(3, &[]);
                let Some(body) = self.read_password_message()? else {
                    self.fatal(protocol_violation("expected password response"))?;
                    return Ok(false);
                };
                let given = body.split(|b| *b == 0).next().unwrap_or_default();
                if users.get(user).is_none_or(|pw| pw.as_bytes() != given) {
                    self.fatal(failed(user))?;
                    return Ok(false);
                }
            }
            AuthData::Scram(users) => {
                let mut mechanisms = scram::MECHANISM.as_bytes().to_vec();
                mechanisms.extend_from_slice(&[0, 0]);
                self.auth_message(10, &mechanisms);
                let Some(body) = self.read_password_message()? else {
                    self.fatal(protocol_violation("expected SASL response"))?;
                    return Ok(false);
                };
                let parsed = (|| -> io::Result<(String, Vec<u8>)> {
                    let mut r = Reader::new(&body);
                    let mechanism = r.cstr()?.to_string();
                    let len = r.i32()?;
                    let data = if len < 0 { Vec::new() } else { r.bytes(len as usize)?.to_vec() };
                    Ok((mechanism, data))
                })();
                let (mechanism, data) = match parsed {
                    Ok(p) => p,
                    Err(_) => {
                        self.fatal(protocol_violation("malformed SASL initial response"))?;
                        return Ok(false);
                    }
                };
                if mechanism != scram::MECHANISM {
                    self.fatal(protocol_violation(format!("unsupported SASL mechanism \"{mechanism}\"")))?;
                    return Ok(false);
                }
                let client_first = String::from_utf8_lossy(&data).into_owned();
                let (exchange, server_first) = match scram::Exchange::start(&client_first, users.get(user).cloned()) {
                    Ok(x) => x,
                    Err(msg) => {
                        self.fatal(protocol_violation(msg))?;
                        return Ok(false);
                    }
                };
                self.auth_message(11, server_first.as_bytes());
                let Some(body) = self.read_password_message()? else {
                    self.fatal(protocol_violation("expected SASL response"))?;
                    return Ok(false);
                };
                match exchange.finish(&String::from_utf8_lossy(&body)) {
                    Ok(Some(server_final)) => self.auth_message(12, server_final.as_bytes()),
                    Ok(None) => {
                        self.fatal(failed(user))?;
                        return Ok(false);
                    }
                    Err(msg) => {
                        self.fatal(protocol_violation(msg))?;
                        return Ok(false);
                    }
                }
            }
        }
        self.auth_message(0, &[]);
        Ok(true)
    }

    /// Handles everything up to the first ReadyForQuery. Returns false if
    /// the connection ends here (cancel request, failed authentication).
    fn startup(&mut self) -> io::Result<bool> {
        let params = loop {
            let (code, payload) = self.io.read_startup()?;
            match code {
                SSL_REQUEST | GSSENC_REQUEST => {
                    // Encryption is not offered; the client may continue
                    // in clear text.
                    self.io.out.push(b'N');
                    self.io.flush()?;
                }
                // Queries cannot be cancelled yet; the request is ignored.
                CANCEL_REQUEST => return Ok(false),
                v if v >> 16 == 3 => {
                    if v & 0xFFFF != 0 {
                        // Newer minor version: negotiate down to 3.0.
                        let m = self.io.begin(b'v');
                        self.io.i32(196608);
                        self.io.i32(0);
                        self.io.end(m);
                    }
                    let mut params = HashMap::new();
                    let mut parts = payload.split(|b| *b == 0);
                    while let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                        if k.is_empty() {
                            break;
                        }
                        params.insert(String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned());
                    }
                    break params;
                }
                other => {
                    self.fatal(protocol_violation(format!(
                        "unsupported frontend protocol {}.{}: server supports 3.0",
                        other >> 16,
                        other & 0xFFFF
                    )))?;
                    return Ok(false);
                }
            }
        };
        let Some(user) = params.get("user").filter(|u| !u.is_empty()).cloned() else {
            self.fatal(Error::new(
                sqlstate::INVALID_AUTHORIZATION_SPECIFICATION,
                "no PostgreSQL user name specified in startup packet",
            ))?;
            return Ok(false);
        };
        if !self.authenticate(&user)? {
            return Ok(false);
        }
        self.session.state_mut().user = user.clone();

        // Run-time parameters passed in the startup packet.
        let mut settings: Vec<(String, String)> = Vec::new();
        if let Some(options) = params.get("options") {
            let mut words = options.split_whitespace();
            while let Some(w) = words.next() {
                let assignment = match w {
                    "-c" => words.next(),
                    w => w.strip_prefix("-c").or_else(|| w.strip_prefix("--")),
                };
                if let Some((k, v)) = assignment.and_then(|a| a.split_once('=')) {
                    settings.push((k.replace('-', "_"), v.to_string()));
                }
            }
        }
        for (k, v) in &params {
            if !matches!(k.as_str(), "user" | "database" | "options" | "replication") && !k.starts_with("_pq_.") {
                settings.push((k.clone(), v.clone()));
            }
        }
        for (name, value) in settings {
            let valid = !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
            if !valid {
                continue;
            }
            let result = self.session.execute(&format!("SET {name} TO '{}'", value.replace('\'', "''")));
            if let (Err(e), true) = (result, name.eq_ignore_ascii_case("client_encoding")) {
                self.fatal(e)?;
                return Ok(false);
            }
        }

        let tz = self.session.state().tz.name().to_string();
        let app = self.session.state().settings.get("application_name").cloned().unwrap_or_default();
        for (name, value) in [
            ("server_version", squad::SERVER_VERSION),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("TimeZone", tz.as_str()),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("application_name", app.as_str()),
            ("is_superuser", "on"),
            ("session_authorization", user.as_str()),
        ] {
            self.parameter_status(name, value);
        }
        self.reported_tz = tz;
        self.reported_app = app;
        let m = self.io.begin(b'K');
        self.io.i32(self.session.state().pid);
        self.io.i32(rand::random::<i32>());
        self.io.end(m);
        Ok(true)
    }

    /// Sends ReadyForQuery, preceded by ParameterStatus for settings the
    /// client tracks that changed.
    fn ready(&mut self) {
        let tz = self.session.state().tz.name();
        if tz != self.reported_tz {
            self.reported_tz = tz.to_string();
            let tz = self.reported_tz.clone();
            self.parameter_status("TimeZone", &tz);
        }
        let app = self.session.state().settings.get("application_name").map_or("", String::as_str);
        if app != self.reported_app {
            self.reported_app = app.to_string();
            let app = self.reported_app.clone();
            self.parameter_status("application_name", &app);
        }
        let status = match self.session.status() {
            TxnStatus::Idle => b'I',
            TxnStatus::InTransaction => b'T',
            TxnStatus::Failed => b'E',
        };
        if status == b'I' {
            // Portals do not outlive their transaction.
            self.portals.clear();
        }
        self.io.message(b'Z', &[status]);
    }

    pub(crate) fn run(mut self) -> io::Result<()> {
        match self.startup() {
            Ok(true) => {}
            Ok(false) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                return self.fatal(protocol_violation(e.to_string()));
            }
            Err(e) => return Err(e),
        }
        self.ready();
        // After an error in the extended protocol, messages are skipped
        // until the next Sync.
        let mut skipping = false;
        loop {
            let (ty, body) = match self.io.read_message() {
                Ok(Some(m)) => m,
                Ok(None) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                    return self.fatal(protocol_violation(e.to_string()));
                }
                Err(e) => return Err(e),
            };
            if skipping && !matches!(ty, b'S' | b'X') {
                continue;
            }
            let step: Step = match ty {
                b'Q' => {
                    let result = match self.simple_query(&body) {
                        Err(Fail::Db(e)) => {
                            error_response(&mut self.io, "ERROR", &e);
                            Ok(())
                        }
                        other => other,
                    };
                    if result.is_ok() {
                        self.ready();
                    }
                    result
                }
                b'P' => self.parse(&body),
                b'B' => self.bind(&body),
                b'D' => self.describe(&body),
                b'E' => self.execute(&body),
                b'C' => self.close(&body),
                b'H' => self.io.flush().map_err(Fail::Io),
                b'S' => {
                    skipping = false;
                    self.ready();
                    Ok(())
                }
                b'X' => return self.io.flush(),
                // Copy data arriving after a failed COPY is dropped.
                b'd' | b'c' | b'f' => Ok(()),
                other => {
                    return self.fatal(protocol_violation(format!("invalid frontend message type {other}")));
                }
            };
            match step {
                Ok(()) => {}
                Err(Fail::Db(e)) => {
                    error_response(&mut self.io, "ERROR", &e);
                    skipping = true;
                }
                Err(Fail::Io(e)) if e.kind() == io::ErrorKind::InvalidData => {
                    return self.fatal(protocol_violation(e.to_string()));
                }
                Err(Fail::Io(e)) => return Err(e),
            }
        }
    }

    // ---------------------------------------------------------------
    // Simple query protocol
    // ---------------------------------------------------------------

    fn simple_query(&mut self, body: &[u8]) -> Step {
        let sql = Reader::new(body).cstr()?;
        if sql.trim().is_empty() {
            self.io.message(b'I', &[]);
            return Ok(());
        }
        if contains_copy(sql) {
            return self.simple_query_with_copy(sql);
        }
        self.run_sql(sql)
    }

    /// Runs SQL text through the session, streaming every result.
    fn run_sql(&mut self, sql: &str) -> Step {
        let ctx = self.fn_ctx();
        let io = &mut self.io;
        self.session.execute_each(sql, &mut |r| {
            if !r.columns.is_empty() {
                row_description(io, &r.columns, &[]);
                for row in &r.rows {
                    data_row(io, &r.columns, row, &[], &ctx)?;
                    if io.out.len() > FLUSH_THRESHOLD {
                        io.flush().map_err(io_as_db)?;
                    }
                }
            }
            command_complete(io, &r.tag);
            Ok(())
        })?;
        Ok(())
    }

    /// A query string that may contain COPY statements is run statement
    /// by statement, because COPY needs a sub-protocol of its own.
    fn simple_query_with_copy(&mut self, sql: &str) -> Step {
        let stmts = squad::bind::parse_sql(sql)?;
        if stmts.len() <= 1 {
            return match copy::parse_copy(sql)? {
                Some(req) => self.run_copy(&req),
                None => self.run_sql(sql),
            };
        }
        for raw in &stmts {
            let start = raw.stmt_location.max(0) as usize;
            let end = if raw.stmt_len > 0 { start + raw.stmt_len as usize } else { sql.len() };
            let text = sql.get(start..end.min(sql.len())).unwrap_or(sql).trim();
            let request = if contains_copy(text) { copy::parse_copy(text)? } else { None };
            match request {
                Some(req) => self.run_copy(&req)?,
                None => self.run_sql(text)?,
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // COPY
    // ---------------------------------------------------------------

    fn run_copy(&mut self, req: &CopyRequest) -> Step {
        if req.from {
            self.copy_in(req)
        } else {
            self.copy_out(req)
        }
    }

    fn copy_response(&mut self, ty: u8, columns: usize) {
        let m = self.io.begin(ty);
        self.io.out.push(0); // textual
        self.io.i16(columns as i16);
        for _ in 0..columns {
            self.io.i16(0);
        }
        self.io.end(m);
    }

    fn copy_out(&mut self, req: &CopyRequest) -> Step {
        let prepared = self.session.prepare(&req.select_sql(), &[])?;
        if !prepared.returns_rows() {
            return Err(Error::syntax("COPY query must return rows").into());
        }
        let result = self.session.execute_prepared(&prepared, &[])?;
        let ctx = self.fn_ctx();
        self.copy_response(b'H', result.columns.len());
        let mut line = Vec::new();
        if req.options.header {
            let names: Vec<Option<&str>> = result.columns.iter().map(|c| Some(c.name.as_str())).collect();
            copy::encode_row(&req.options, &names, &mut line);
            self.io.message(b'd', &line);
        }
        for row in &result.rows {
            let cells: Vec<Option<String>> =
                row.iter().map(|v| (!v.is_null()).then(|| format_value(v, &ctx))).collect();
            let fields: Vec<Option<&str>> = cells.iter().map(Option::as_deref).collect();
            line.clear();
            copy::encode_row(&req.options, &fields, &mut line);
            self.io.message(b'd', &line);
            if self.io.out.len() > FLUSH_THRESHOLD {
                self.io.flush()?;
            }
        }
        self.io.message(b'c', &[]);
        command_complete(&mut self.io, &format!("COPY {}", result.rows.len()));
        Ok(())
    }

    fn copy_in(&mut self, req: &CopyRequest) -> Step {
        let columns: Vec<String> = if req.columns.is_empty() {
            self.session.prepare(&req.select_sql(), &[])?.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            req.columns.clone()
        };
        // The parameter types of the INSERT are the column types.
        let single = self.session.prepare(&req.insert_sql(&columns, 1), &[])?;
        let types = single.param_types.clone();
        let batch = self.session.prepare(&req.insert_sql(&columns, COPY_BATCH), &[])?;
        let ctx = self.fn_ctx();

        self.copy_response(b'G', columns.len());
        // Load atomically: in the caller's transaction if there is one,
        // otherwise in a transaction of its own.
        let own_txn = self.session.status() == TxnStatus::Idle;
        if own_txn {
            self.session.execute("BEGIN")?;
        }
        let result = self.copy_in_rows(req, &columns, &types, &single, &batch, &ctx);
        match result {
            Ok(count) => {
                if own_txn {
                    self.session.execute("COMMIT")?;
                }
                command_complete(&mut self.io, &format!("COPY {count}"));
                Ok(())
            }
            Err(e) => {
                if own_txn {
                    let _ = self.session.execute("ROLLBACK");
                }
                Err(e)
            }
        }
    }

    fn copy_in_rows(
        &mut self,
        req: &CopyRequest,
        columns: &[String],
        types: &[Type],
        single: &Arc<Prepared>,
        batch: &Arc<Prepared>,
        ctx: &FnCtx,
    ) -> Step<u64> {
        let mut decoder = CopyDecoder::new(&req.options);
        let mut pending: Vec<Value> = Vec::with_capacity(columns.len() * COPY_BATCH);
        let mut count = 0u64;
        let mut line = 0u64;
        let mut finished = false;
        // A database error does not end the sub-protocol: the client keeps
        // sending until it has seen the error, so the remaining copy
        // messages are consumed (and dropped) by the main loop.
        while !finished {
            match self.io.read_message()? {
                Some((b'd', data)) => decoder.feed(&data),
                Some((b'c', _)) => finished = true,
                Some((b'f', body)) => {
                    let reason =
                        String::from_utf8_lossy(body.split(|b| *b == 0).next().unwrap_or_default()).into_owned();
                    return Err(
                        Error::new(sqlstate::QUERY_CANCELED, format!("COPY from stdin failed: {reason}")).into()
                    );
                }
                Some((b'H' | b'S', _)) => continue,
                Some((other, _)) => {
                    return Err(
                        protocol_violation(format!("unexpected message type {other} during COPY from stdin")).into()
                    )
                }
                None => return Err(Fail::Io(io::ErrorKind::UnexpectedEof.into())),
            }
            while let Some(fields) = decoder.next_row(finished)? {
                line += 1;
                if fields.len() != columns.len() {
                    let msg = if fields.len() < columns.len() {
                        format!("missing data for column \"{}\"", columns[fields.len()])
                    } else {
                        "extra data after last expected column".to_string()
                    };
                    return Err(Error::new("22P04", msg).with_detail(format!("COPY line {line}")).into());
                }
                for (field, ty) in fields.into_iter().zip(types) {
                    pending.push(match (field, ty) {
                        (None, _) => Value::Null,
                        (Some(s), Type::Text | Type::Enum(_)) => Value::Text(s),
                        (Some(s), ty) => {
                            parse_value(&s, ty, ctx).map_err(|e| e.with_detail(format!("COPY line {line}")))?
                        }
                    });
                }
                if pending.len() == columns.len() * COPY_BATCH {
                    count += self.session.execute_prepared(batch, &pending)?.rows_affected;
                    pending.clear();
                }
            }
        }
        for row in pending.chunks(columns.len().max(1)) {
            count += self.session.execute_prepared(single, row)?.rows_affected;
        }
        Ok(count)
    }

    // ---------------------------------------------------------------
    // Extended query protocol
    // ---------------------------------------------------------------

    fn parse(&mut self, body: &[u8]) -> Step {
        let mut r = Reader::new(body);
        let name = r.cstr()?.to_string();
        let sql = r.cstr()?;
        let n = r.i16()?.max(0) as usize;
        let mut types = Vec::with_capacity(n);
        for _ in 0..n {
            let oid = r.i32()? as u32;
            types.push(if oid == 0 { None } else { Type::from_oid(oid).filter(|t| *t != Type::Unknown) });
        }
        if !name.is_empty() && self.stmts.contains_key(&name) {
            return Err(Error::new(
                sqlstate::DUPLICATE_PREPARED_STATEMENT,
                format!("prepared statement \"{name}\" already exists"),
            )
            .into());
        }
        let request = if contains_copy(sql) { copy::parse_copy(sql)? } else { None };
        let kind = match request {
            Some(req) => StmtKind::Copy(Box::new(req)),
            None => StmtKind::Sql(self.session.prepare(sql, &types)?),
        };
        self.stmts.insert(name, Stmt { kind });
        self.io.message(b'1', &[]);
        Ok(())
    }

    fn statement(&self, name: &str) -> Result<&Stmt, Error> {
        self.stmts.get(name).ok_or_else(|| {
            Error::new(
                sqlstate::INVALID_SQL_STATEMENT_NAME,
                if name.is_empty() {
                    "unnamed prepared statement does not exist".to_string()
                } else {
                    format!("prepared statement \"{name}\" does not exist")
                },
            )
        })
    }

    fn bind(&mut self, body: &[u8]) -> Step {
        let mut r = Reader::new(body);
        let portal = r.cstr()?.to_string();
        let stmt_name = r.cstr()?.to_string();
        let nformats = r.i16()?.max(0) as usize;
        let mut param_formats = Vec::with_capacity(nformats);
        for _ in 0..nformats {
            param_formats.push(r.i16()?);
        }
        let nparams = r.i16()?.max(0) as usize;
        let kind = self.statement(&stmt_name)?.kind.clone();
        let empty: [Type; 0] = [];
        let types: &[Type] = match &kind {
            StmtKind::Sql(p) => &p.param_types,
            StmtKind::Copy(_) => &empty,
        };
        if nparams != types.len() {
            return Err(protocol_violation(format!(
                "bind message supplies {nparams} parameters, but prepared statement \"{stmt_name}\" requires {}",
                types.len()
            ))
            .into());
        }
        if nformats > 1 && nformats != nparams {
            return Err(protocol_violation(format!(
                "bind message has {nformats} parameter formats but {nparams} parameters"
            ))
            .into());
        }
        let ctx = self.fn_ctx();
        let mut params = Vec::with_capacity(nparams);
        for (i, ty) in types.iter().enumerate() {
            let len = r.i32()?;
            if len < 0 {
                params.push(Value::Null);
                continue;
            }
            let bytes = r.bytes(len as usize)?;
            params.push(if format_at(&param_formats, i) == 1 {
                decode_binary(bytes, ty)?
            } else {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    Error::new(sqlstate::CHARACTER_NOT_IN_REPERTOIRE, "invalid byte sequence for encoding \"UTF8\"")
                })?;
                match ty {
                    Type::Text | Type::Enum(_) => Value::Text(text.to_string()),
                    ty => parse_value(text, ty, &ctx)?,
                }
            });
        }
        let nresults = r.i16()?.max(0) as usize;
        let mut formats = Vec::with_capacity(nresults);
        for _ in 0..nresults {
            formats.push(r.i16()?);
        }
        if !portal.is_empty() && self.portals.contains_key(&portal) {
            return Err(Error::new("42P03", format!("cursor \"{portal}\" already exists")).into());
        }
        self.portals.insert(portal, Portal { kind, params, formats, result: None });
        self.io.message(b'2', &[]);
        Ok(())
    }

    fn describe(&mut self, body: &[u8]) -> Step {
        let mut r = Reader::new(body);
        let what = r.u8()?;
        let name = r.cstr()?;
        match what {
            b'S' => {
                let kind = self.statement(name)?.kind.clone();
                let (params, columns): (&[Type], &[OutputColumn]) = match &kind {
                    StmtKind::Sql(p) => (&p.param_types, &p.columns),
                    StmtKind::Copy(_) => (&[], &[]),
                };
                let m = self.io.begin(b't');
                self.io.i16(params.len() as i16);
                for t in params {
                    self.io.i32(t.oid() as i32);
                }
                self.io.end(m);
                if columns.is_empty() {
                    self.io.message(b'n', &[]);
                } else {
                    row_description(&mut self.io, columns, &[]);
                }
            }
            b'P' => {
                let portal = self.portals.get(name).ok_or_else(|| {
                    Error::new(sqlstate::INVALID_CURSOR_NAME, format!("portal \"{name}\" does not exist"))
                })?;
                match &portal.kind {
                    StmtKind::Sql(p) if !p.columns.is_empty() => {
                        row_description(&mut self.io, &p.columns, &portal.formats)
                    }
                    _ => self.io.message(b'n', &[]),
                }
            }
            other => return Err(protocol_violation(format!("invalid DESCRIBE message subtype {other}")).into()),
        }
        Ok(())
    }

    fn execute(&mut self, body: &[u8]) -> Step {
        let mut r = Reader::new(body);
        let name = r.cstr()?;
        let max_rows = r.i32()?.max(0) as usize;
        let ctx = self.fn_ctx();
        let portal = self
            .portals
            .get_mut(name)
            .ok_or_else(|| Error::new(sqlstate::INVALID_CURSOR_NAME, format!("portal \"{name}\" does not exist")))?;
        let prepared = match &portal.kind {
            StmtKind::Copy(req) => {
                let req = req.clone();
                return self.run_copy(&req);
            }
            StmtKind::Sql(p) => p.clone(),
        };
        if portal.result.is_none() {
            let result = self.session.execute_prepared(&prepared, &portal.params)?;
            portal.result = Some((result, 0));
        }
        let (result, sent) = portal.result.as_mut().expect("portal was just executed");
        let end = if max_rows == 0 { result.rows.len() } else { (*sent + max_rows).min(result.rows.len()) };
        for row in &result.rows[*sent..end] {
            data_row(&mut self.io, &result.columns, row, &portal.formats, &ctx)?;
            if self.io.out.len() > FLUSH_THRESHOLD {
                self.io.flush()?;
            }
        }
        *sent = end;
        if end < result.rows.len() {
            self.io.message(b's', &[]);
        } else {
            command_complete(&mut self.io, &result.tag);
        }
        Ok(())
    }

    fn close(&mut self, body: &[u8]) -> Step {
        let mut r = Reader::new(body);
        let what = r.u8()?;
        let name = r.cstr()?;
        match what {
            b'S' => {
                self.stmts.remove(name);
            }
            b'P' => {
                self.portals.remove(name);
            }
            other => return Err(protocol_violation(format!("invalid CLOSE message subtype {other}")).into()),
        }
        self.io.message(b'3', &[]);
        Ok(())
    }
}

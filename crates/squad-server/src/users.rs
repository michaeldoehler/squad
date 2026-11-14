//! Login credentials from a users file.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::scram::Verifier;

/// Parses the contents of a users file: one `user:secret` per line, where
/// the secret is either a password or a SCRAM-SHA-256 verifier in
/// PostgreSQL's format (`SCRAM-SHA-256$4096:salt$StoredKey:ServerKey`, as
/// stored in `pg_authid.rolpassword` and printed by `squad
/// hash-password`). Empty lines and lines starting with `#` are ignored.
/// Passwords are turned into verifiers right away, so only verifiers stay
/// in memory.
pub fn parse_users(text: &str) -> Result<HashMap<String, Verifier>, String> {
    let mut users = HashMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let Some((user, secret)) = line.split_once(':') else {
            return Err(format!("line {}: expected user:password or user:SCRAM-SHA-256$…", n + 1));
        };
        if user.is_empty() {
            return Err(format!("line {}: empty user name", n + 1));
        }
        if secret.is_empty() {
            return Err(format!("line {}: empty password for user \"{user}\"", n + 1));
        }
        let verifier = if secret.starts_with("SCRAM-SHA-256$") {
            Verifier::parse(secret).map_err(|e| format!("line {}: {e}", n + 1))?
        } else {
            Verifier::from_password(secret)
        };
        if users.insert(user.to_string(), verifier).is_some() {
            return Err(format!("line {}: user \"{user}\" appears twice", n + 1));
        }
    }
    Ok(users)
}

/// Reads a users file; see [`parse_users`].
pub fn read_users_file(path: impl AsRef<Path>) -> io::Result<HashMap<String, Verifier>> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path)
        .map_err(|e| io::Error::new(e.kind(), format!("cannot read users file {}: {e}", path.display())))?;
    parse_users(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_and_verifier_lines() {
        let pg = "SCRAM-SHA-256$4096:2p69YhUMsuJ9UY9GhURkFg==$btRVJP5LmOQTVAAf2sXJeDPPa4dj3mYOSahIsXWl8SU=:VQGgrY+lvmyvr0xiP+b7VjMzf4JJlB+lf1XRYBAA4Yc=";
        let text = format!("# logins\n\napp:pass:with:colons\r\nreporting:{pg}\n");
        let users = parse_users(&text).unwrap();
        assert_eq!(users.len(), 2);
        assert!(users["app"].matches_password("pass:with:colons"));
        assert!(users["reporting"].matches_password("secret"));
        assert!(!users["reporting"].matches_password("pass:with:colons"));
    }

    #[test]
    fn errors_name_the_line() {
        for (text, msg) in [
            ("app\n", "line 1: expected user:password"),
            ("a:x\n:pw\n", "line 2: empty user name"),
            ("app:\n", "line 1: empty password"),
            ("a:x\na:y\n", "line 2: user \"a\" appears twice"),
            ("a:SCRAM-SHA-256$broken\n", "line 1: malformed SCRAM-SHA-256 verifier"),
        ] {
            let err = parse_users(text).unwrap_err();
            assert!(err.starts_with(msg), "{text:?}: {err}");
        }
    }
}

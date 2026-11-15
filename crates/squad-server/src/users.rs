//! Login credentials from a users file.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::scram::Verifier;
use crate::LoginAttributes;

/// The logins of a users file.
#[derive(Debug, Clone, Default)]
pub struct Users {
    /// User name to verifier, for [`Auth::Verifiers`](crate::Auth::Verifiers).
    pub verifiers: HashMap<String, Verifier>,
    /// Attributes of the logins that have any, for
    /// [`ServerConfig::logins`](crate::ServerConfig::logins).
    pub logins: HashMap<String, LoginAttributes>,
}

/// Parses the contents of a users file: one `user:secret` per line, where
/// the secret is either a password or a SCRAM-SHA-256 verifier in
/// PostgreSQL's format (`SCRAM-SHA-256$4096:salt$StoredKey:ServerKey`, as
/// stored in `pg_authid.rolpassword` and printed by `squad
/// hash-password`). Attributes may follow the user name, separated by
/// spaces: `reporting read_only no_bypass_rls:secret` (see
/// [`LoginAttributes`]). Empty lines and lines starting with `#` are
/// ignored. Passwords are turned into verifiers right away, so only
/// verifiers stay in memory.
pub fn parse_users(text: &str) -> Result<Users, String> {
    let mut users = Users::default();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let Some((head, secret)) = line.split_once(':') else {
            return Err(format!("line {}: expected user:password or user:SCRAM-SHA-256$…", n + 1));
        };
        let mut words = head.split_whitespace();
        let user = words.next().unwrap_or_default();
        if user.is_empty() {
            return Err(format!("line {}: empty user name", n + 1));
        }
        let mut attributes = LoginAttributes::default();
        for word in words {
            match word {
                "read_only" => attributes.read_only = true,
                "no_bypass_rls" => attributes.bypass_rls = false,
                other => {
                    return Err(format!(
                        "line {}: unknown attribute \"{other}\" for user \"{user}\" (known: read_only, no_bypass_rls)",
                        n + 1
                    ))
                }
            }
        }
        if secret.is_empty() {
            return Err(format!("line {}: empty password for user \"{user}\"", n + 1));
        }
        let verifier = if secret.starts_with("SCRAM-SHA-256$") {
            Verifier::parse(secret).map_err(|e| format!("line {}: {e}", n + 1))?
        } else {
            Verifier::from_password(secret)
        };
        if users.verifiers.insert(user.to_string(), verifier).is_some() {
            return Err(format!("line {}: user \"{user}\" appears twice", n + 1));
        }
        if attributes != LoginAttributes::default() {
            users.logins.insert(user.to_string(), attributes);
        }
    }
    Ok(users)
}

/// Reads a users file; see [`parse_users`].
pub fn read_users_file(path: impl AsRef<Path>) -> io::Result<Users> {
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
        let users = parse_users(&text).unwrap().verifiers;
        assert_eq!(users.len(), 2);
        assert!(users["app"].matches_password("pass:with:colons"));
        assert!(users["reporting"].matches_password("secret"));
        assert!(!users["reporting"].matches_password("pass:with:colons"));
    }

    #[test]
    fn attributes_follow_the_user_name() {
        let text = "app:pw\nsuperset read_only  no_bypass_rls:p:w\ntenant no_bypass_rls:x\n";
        let users = parse_users(text).unwrap();
        assert_eq!(users.verifiers.len(), 3);
        assert!(users.verifiers["superset"].matches_password("p:w"));
        assert!(!users.logins.contains_key("app"));
        assert_eq!(users.logins["superset"], LoginAttributes { bypass_rls: false, read_only: true });
        assert_eq!(users.logins["tenant"], LoginAttributes { bypass_rls: false, read_only: false });
        let err = parse_users("bob superuser:pw\n").unwrap_err();
        assert!(err.starts_with("line 1: unknown attribute \"superuser\""), "{err}");
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

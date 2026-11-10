//! SCRAM-SHA-256 (RFC 5802, RFC 7677), server side.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

pub const MECHANISM: &str = "SCRAM-SHA-256";
pub const ITERATIONS: u32 = 4096;

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// What the server stores per user instead of the password.
#[derive(Clone)]
pub struct Verifier {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl Verifier {
    /// Derives a verifier from a password with a fresh random salt.
    pub fn from_password(password: &str) -> Verifier {
        let mut salt = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        Verifier::with_salt(password, salt, ITERATIONS)
    }

    pub fn with_salt(password: &str, salt: Vec<u8>, iterations: u32) -> Verifier {
        let salted = salted_password(password, &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let server_key = hmac(&salted, b"Server Key");
        Verifier { salt, iterations, stored_key, server_key }
    }
}

pub fn salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), salt, iterations, &mut out).expect("valid output length");
    out
}

/// Computes the client proof and the expected server signature; used by
/// clients (and by the tests of this crate).
pub fn client_proof(password: &str, salt: &[u8], iterations: u32, auth_message: &str) -> ([u8; 32], [u8; 32]) {
    let salted = salted_password(password, salt, iterations);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let signature = hmac(&stored_key, auth_message.as_bytes());
    let mut proof = client_key;
    for (p, s) in proof.iter_mut().zip(signature) {
        *p ^= s;
    }
    (proof, hmac(&hmac(&salted, b"Server Key"), auth_message.as_bytes()))
}

/// State of one authentication exchange.
pub struct Exchange {
    verifier: Verifier,
    /// False if the user is unknown: the exchange runs to the end with a
    /// throwaway verifier and then fails, so user names cannot be probed.
    known_user: bool,
    client_first_bare: String,
    server_first: String,
    nonce: String,
}

fn attribute(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|part| part.strip_prefix(key).and_then(|rest| rest.strip_prefix('=')))
}

impl Exchange {
    /// Processes the client-first message and returns the exchange state
    /// with the server-first message to send.
    pub fn start(client_first: &str, verifier: Option<Verifier>) -> Result<(Exchange, String), String> {
        // gs2 header: "n,," or "y,," (no channel binding), optional authzid.
        let mut parts = client_first.splitn(3, ',');
        let binding = parts.next().unwrap_or_default();
        let _authzid = parts.next().ok_or("malformed SCRAM client-first message")?;
        let bare = parts.next().ok_or("malformed SCRAM client-first message")?;
        if binding != "n" && binding != "y" {
            return Err("SCRAM channel binding is not supported".into());
        }
        let client_nonce =
            attribute(bare, 'r').filter(|n| !n.is_empty()).ok_or("SCRAM client-first message lacks a nonce")?;
        let mut raw = [0u8; 18];
        rand::thread_rng().fill_bytes(&mut raw);
        let nonce = format!("{client_nonce}{}", B64.encode(raw));
        let known_user = verifier.is_some();
        let verifier = verifier.unwrap_or_else(|| Verifier::with_salt("", raw[..16].to_vec(), ITERATIONS));
        let server_first = format!("r={nonce},s={},i={}", B64.encode(&verifier.salt), verifier.iterations);
        Ok((
            Exchange {
                verifier,
                known_user,
                client_first_bare: bare.to_string(),
                server_first: server_first.clone(),
                nonce,
            },
            server_first,
        ))
    }

    /// Verifies the client-final message. Returns the server-final message
    /// on success and `None` if the proof (or the user) is wrong.
    pub fn finish(&self, client_final: &str) -> Result<Option<String>, String> {
        let proof_at = client_final.rfind(",p=").ok_or("SCRAM client-final message lacks a proof")?;
        let without_proof = &client_final[..proof_at];
        let proof = B64.decode(&client_final[proof_at + 3..]).map_err(|_| "malformed SCRAM proof")?;
        if attribute(without_proof, 'r') != Some(self.nonce.as_str()) {
            return Err("SCRAM nonce mismatch".into());
        }
        match attribute(without_proof, 'c') {
            Some("biws" | "eSws") => {}
            _ => return Err("unexpected SCRAM channel binding data".into()),
        }
        if proof.len() != 32 {
            return Ok(None);
        }
        let auth_message = format!("{},{},{}", self.client_first_bare, self.server_first, without_proof);
        let signature = hmac(&self.verifier.stored_key, auth_message.as_bytes());
        let mut client_key = [0u8; 32];
        for (i, k) in client_key.iter_mut().enumerate() {
            *k = proof[i] ^ signature[i];
        }
        let computed: [u8; 32] = Sha256::digest(client_key).into();
        // Constant-time comparison.
        let diff = computed.iter().zip(self.verifier.stored_key).fold(0u8, |acc, (a, b)| acc | (a ^ b));
        if diff != 0 || !self.known_user {
            return Ok(None);
        }
        let server_signature = hmac(&self.verifier.server_key, auth_message.as_bytes());
        Ok(Some(format!("v={}", B64.encode(server_signature))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc7677_test_vector() {
        // Example from RFC 7677, section 3.
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let auth = "n=user,r=rOprNGfwEbeRWgbNEkqO,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let (proof, server) = client_proof("pencil", &salt, 4096, auth);
        assert_eq!(B64.encode(proof), "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
        assert_eq!(B64.encode(server), "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    }

    #[test]
    fn exchange_accepts_right_and_rejects_wrong_password() {
        for (password, ok) in [("secret", true), ("wrong", false)] {
            let verifier = Verifier::from_password("secret");
            let bare = "n=,r=clientnonce";
            let (ex, server_first) = Exchange::start(&format!("n,,{bare}"), Some(verifier)).unwrap();
            let nonce = attribute(&server_first, 'r').unwrap();
            let salt = B64.decode(attribute(&server_first, 's').unwrap()).unwrap();
            let without_proof = format!("c=biws,r={nonce}");
            let auth = format!("{bare},{server_first},{without_proof}");
            let (proof, server_sig) = client_proof(password, &salt, ITERATIONS, &auth);
            let result = ex.finish(&format!("{without_proof},p={}", B64.encode(proof))).unwrap();
            assert_eq!(result.is_some(), ok);
            if let Some(fin) = result {
                assert_eq!(fin, format!("v={}", B64.encode(server_sig)));
            }
        }
        // Unknown users fail even with a "correct" proof for the fake verifier.
        let (ex, _) = Exchange::start("n,,n=x,r=abc", None).unwrap();
        assert!(ex.finish(&format!("c=biws,r={},p={}", ex.nonce, B64.encode([0u8; 32]))).unwrap().is_none());
    }
}

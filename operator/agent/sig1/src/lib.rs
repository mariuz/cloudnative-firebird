//! Signed requests to the segment servers.
//!
//! A request line is `SIG1 <epoch> <nonce> <mac> <request>`, where `mac` is the hex HMAC-SHA256 of
//! `"<epoch> <nonce> <request>"` keyed with the SYSDBA password. The password never crosses the
//! network. The same format is implemented in Perl (`segment-auth.pl`, `segment-server.pl`) and in
//! TypeScript (`replication-lag.ts`); the conformance tests run the three against each other.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::fmt;

type HmacSha256 = Hmac<Sha256>;

fn mac(secret: &[u8], at: u64, nonce: &str, request: &str) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC takes a key of any length");
    mac.update(format!("{at} {nonce} {request}").as_bytes());
    mac
}

/// The request line for `request`, signed at `at` (epoch seconds) with `nonce` (16 to 64 hex digits).
pub fn sign(secret: &[u8], request: &str, at: u64, nonce: &str) -> String {
    let digest = mac(secret, at, nonce, request).finalize().into_bytes();
    format!("SIG1 {at} {nonce} {} {request}", hex(&digest))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !is_lower_hex(text) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

fn is_lower_hex(text: &str) -> bool {
    text.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Why a request line is refused, as the segment server names it in `ERR unauthorized (<reason>)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    /// Not a `SIG1` line.
    Unsigned,
    /// The signature is not the one of this secret.
    Signature,
    /// Signed further from the server's clock than the window allows.
    Clock,
    /// A nonce seen before.
    Replay,
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Denied::Unsigned => "unsigned",
            Denied::Signature => "signature",
            Denied::Clock => "clock",
            Denied::Replay => "replay",
        })
    }
}

/// A verified request: its command, and the rest of the line if there is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub command: String,
    pub argument: Option<String>,
}

/// Verifies request lines as `segment-server.pl` does: the signature first, then the clock, then
/// the nonce, which is remembered for the window.
pub struct Verifier {
    secret: Vec<u8>,
    window: u64,
    nonces: HashMap<String, u64>,
}

impl Verifier {
    /// `window`: how far a request's time may be from the server's clock, in seconds (300 by default).
    pub fn new(secret: &[u8], window: u64) -> Self {
        Verifier {
            secret: secret.to_vec(),
            window,
            nonces: HashMap::new(),
        }
    }

    pub fn verify(&mut self, line: &str, now: u64) -> Result<Request, Denied> {
        let (at, nonce, sent, request) = parse(line).ok_or(Denied::Unsigned)?;
        // compared in constant time
        let sent = unhex(sent).ok_or(Denied::Unsigned)?;
        mac(&self.secret, at, nonce, request)
            .verify_slice(&sent)
            .map_err(|_| Denied::Signature)?;
        if now.abs_diff(at) > self.window {
            return Err(Denied::Clock);
        }
        if self.nonces.contains_key(nonce) {
            return Err(Denied::Replay);
        }
        self.nonces.insert(nonce.to_string(), at);
        if self.nonces.len() > 1000 {
            let window = self.window;
            self.nonces.retain(|_, seen| now.saturating_sub(*seen) <= window);
        }
        let (command, argument) = match request.split_once(' ') {
            Some((command, argument)) => (command, Some(argument.to_string())),
            None => (request, None),
        };
        Ok(Request {
            command: command.to_string(),
            argument,
        })
    }
}

/// `SIG1 (\d+) ([0-9a-f]{16,64}) ([0-9a-f]{64}) (.+)`: the time, the nonce, the mac and the request.
fn parse(line: &str) -> Option<(u64, &str, &str, &str)> {
    let rest = line.strip_prefix("SIG1 ")?;
    let (at, rest) = rest.split_once(' ')?;
    let (nonce, rest) = rest.split_once(' ')?;
    let (mac, request) = rest.split_once(' ')?;
    if at.is_empty() || !at.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !(16..=64).contains(&nonce.len()) || !is_lower_hex(nonce) || mac.len() != 64 || !is_lower_hex(mac) || request.is_empty() {
        return None;
    }
    Some((at.parse().ok()?, nonce, mac, request))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONCE: &str = "00112233445566778899aabbccddeeff";

    #[test]
    fn signs_as_the_node_and_perl_clients() {
        // createHmac('sha256', 'tok').update('1700000000 <nonce> PING').digest('hex')
        assert_eq!(
            sign(b"tok", "PING", 1_700_000_000, NONCE),
            format!("SIG1 1700000000 {NONCE} 66fe0169da023b7f212c9bf75f0ebac88ec4c951c00b12f1e6fffb1e9b0abdc3 PING")
        );
        // an empty secret, and a request with an argument
        assert_eq!(
            sign(b"", "GET mydb.fdb.journal-000000007", 1_700_000_000, NONCE),
            format!(
                "SIG1 1700000000 {NONCE} 1918be96c87e0e9af7508905f3992e09fe2935f974d943d8dc7057967cae61fc GET mydb.fdb.journal-000000007"
            )
        );
    }

    #[test]
    fn verifies_a_signed_request() {
        let mut verifier = Verifier::new(b"tok", 300);
        let request = verifier.verify(&sign(b"tok", "TXNS 7 1,2", 1000, NONCE), 1100).unwrap();
        assert_eq!(
            request,
            Request {
                command: "TXNS".into(),
                argument: Some("7 1,2".into())
            }
        );
        let request = verifier
            .verify(&sign(b"tok", "PING", 1000, "ffeeddccbbaa99887766554433221100"), 900)
            .unwrap();
        assert_eq!(
            request,
            Request {
                command: "PING".into(),
                argument: None
            }
        );
    }

    #[test]
    fn refuses_as_the_segment_server() {
        let mut verifier = Verifier::new(b"tok", 300);
        assert_eq!(verifier.verify("tok PING", 1000), Err(Denied::Unsigned));
        assert_eq!(verifier.verify("SIG1 1000 abc 00 PING", 1000), Err(Denied::Unsigned));
        assert_eq!(verifier.verify(&sign(b"other", "PING", 1000, NONCE), 1000), Err(Denied::Signature));
        // the signature covers the request
        let moved = sign(b"tok", "PING", 1000, NONCE).replace("PING", "SEED");
        assert_eq!(verifier.verify(&moved, 1000), Err(Denied::Signature));
        // the signature is checked before the clock, the clock before the nonce
        assert_eq!(verifier.verify(&sign(b"other", "PING", 1000, NONCE), 9000), Err(Denied::Signature));
        assert_eq!(verifier.verify(&sign(b"tok", "PING", 1000, NONCE), 1301), Err(Denied::Clock));
        assert_eq!(verifier.verify(&sign(b"tok", "PING", 1000, NONCE), 699), Err(Denied::Clock));
        assert!(verifier.verify(&sign(b"tok", "PING", 1000, NONCE), 1300).is_ok());
        assert_eq!(verifier.verify(&sign(b"tok", "PING", 1000, NONCE), 1300), Err(Denied::Replay));
        assert_eq!(Denied::Replay.to_string(), "replay");
    }

    #[test]
    fn forgets_nonces_older_than_the_window() {
        let mut verifier = Verifier::new(b"tok", 10);
        for i in 0..1001u64 {
            assert!(verifier.verify(&sign(b"tok", "PING", 1000, &format!("{i:032x}")), 1000).is_ok());
        }
        // more than 1000 remembered: the next request, past the window of the others, drops them
        assert!(verifier
            .verify(&sign(b"tok", "PING", 1020, &format!("{:032x}", 5000)), 1020)
            .is_ok());
        assert_eq!(verifier.nonces.len(), 1);
    }
}

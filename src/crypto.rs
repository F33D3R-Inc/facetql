use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use std::sync::OnceLock;

/// An obviously-fake, all-zero key — used only when ENOCHIAN_MASTER_KEY
/// isn't set, so local development doesn't need any setup. Printed as a
/// loud warning every time it's used, same pattern as the dev API token
/// in auth.rs. Anything encrypted with this key is not secure — it's a
/// known, public value.
const DEV_KEY_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub const MASTER_KEY_ENV: &str = "ENOCHIAN_MASTER_KEY";

/// Why the at-rest key must not be used as configured.
///
/// The counterpart to `auth::CredentialDefect`, and for the same reason:
/// [`cipher`] falls back to a key of thirty-two zero bytes, prints a
/// warning, and encrypts the entire database under a value anyone can
/// type from memory. Every heap segment, every WAL frame and every
/// history entry written under it is plaintext to whoever obtains the
/// files — a backup tarball, a snapshot, a decommissioned disk.
///
/// Reporting the defect here rather than deciding it here keeps one
/// module in charge of what a key is: `config::deployment` decides
/// whether a finding is fatal and `main` renders it, but only this file
/// knows what the development key is or what shape a real one has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyDefect {
    /// `ENOCHIAN_MASTER_KEY` is unset, so the all-zero development key
    /// would be used.
    NotConfigured,

    /// The variable is set but is not 64 hex characters. [`cipher`]
    /// panics on this, which is technically fail-closed and arrives as
    /// an unhandled panic at the first write rather than as a diagnosis
    /// at start-up. Reported here so it comes out as the former.
    Malformed(String),

    /// The variable is set to a key that is entirely zero bytes — the
    /// development key written out longhand, which the unset case would
    /// have used anyway.
    AllZero,
}

impl std::fmt::Display for KeyDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyDefect::NotConfigured => write!(
                f,
                "{MASTER_KEY_ENV} is not set, so every record, write-ahead \
                 log frame and history entry would be encrypted under the \
                 all-zero development key — which is to say, not encrypted"
            ),

            KeyDefect::Malformed(why) => write!(
                f,
                "{MASTER_KEY_ENV} is not a usable AES-256 key: {why}"
            ),

            KeyDefect::AllZero => write!(
                f,
                "{MASTER_KEY_ENV} is set to the all-zero development key, \
                 which is a published constant and protects nothing"
            ),
        }
    }
}

/// What is wrong with the configured master key, if anything.
///
/// Reads the environment rather than [`cipher`] for the same reason
/// `auth::credential_defect` does not touch the token map: `cipher` is a
/// `OnceLock` that emits the fallback warning and installs the dev key
/// on first use, and a check must be able to report that outcome without
/// causing it.
pub fn key_defect() -> Option<KeyDefect> {
    key_defect_in(std::env::var(MASTER_KEY_ENV).ok().as_deref())
}

/// The judgement itself, separated from where the string came from — see
/// `auth::credential_defect_in` for why.
fn key_defect_in(raw: Option<&str>) -> Option<KeyDefect> {
    let raw = match raw {
        Some(raw) => raw,
        None => return Some(KeyDefect::NotConfigured),
    };

    let bytes = match decode_hex(raw) {
        Ok(bytes) => bytes,
        Err(why) => return Some(KeyDefect::Malformed(why)),
    };

    if bytes.len() != 32 {
        return Some(KeyDefect::Malformed(format!(
            "it decodes to {} bytes, not the 32 AES-256 requires",
            bytes.len()
        )));
    }

    // Not a string comparison against DEV_KEY_HEX: `0x0000…` and
    // `0X0000…` and a value with surrounding whitespace are all the same
    // key, and the property that makes it unusable is that it is all
    // zeroes, not that it is spelled a particular way.
    if bytes.iter().all(|byte| *byte == 0) {
        return Some(KeyDefect::AllZero);
    }

    None
}

fn cipher() -> &'static Aes256Gcm {
    static CIPHER: OnceLock<Aes256Gcm> = OnceLock::new();
    CIPHER.get_or_init(|| {
        let key_hex = std::env::var(MASTER_KEY_ENV).unwrap_or_else(|_| {
            eprintln!(
                "warning: ENOCHIAN_MASTER_KEY not set — using an all-zero dev key. \
                 Data encrypted with this key is NOT secure. Generate a real key with \
                 `openssl rand -hex 32` and set ENOCHIAN_MASTER_KEY to it before running \
                 with real data."
            );
            DEV_KEY_HEX[..64].to_string()
        });

        let key_bytes = decode_hex(&key_hex).unwrap_or_else(|e| {
            panic!("ENOCHIAN_MASTER_KEY is not valid hex: {e}")
        });

        if key_bytes.len() != 32 {
            panic!(
                "ENOCHIAN_MASTER_KEY must decode to exactly 32 bytes (64 hex characters) \
                 for AES-256 — got {} bytes. Generate one with `openssl rand -hex 32`.",
                key_bytes.len()
            );
        }

        Aes256Gcm::new_from_slice(&key_bytes).expect("key is exactly 32 bytes, checked above")
    })
}

/// Encrypts `plaintext` with AES-256-GCM under a fresh random 12-byte
/// nonce, and returns `nonce || ciphertext` as one blob — the nonce
/// travels with the ciphertext (it isn't secret, it just must never
/// repeat under the same key, which a fresh random one per call
/// guarantees with overwhelming probability). GCM's authentication tag
/// is included in the ciphertext output automatically, so tampering
/// with a stored record is detected on decrypt, not silently accepted.
pub fn encrypt(plaintext: &[u8]) -> Vec<u8> {
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher()
        .encrypt(nonce, plaintext)
        .expect("AES-GCM encryption failed — should be infallible for valid key/nonce/plaintext");

    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    out
}

/// Reverses `encrypt`. Fails if the blob is too short to contain a
/// nonce, if it was encrypted under a different key, or if it's been
/// tampered with — GCM's authentication check catches modification,
/// it doesn't just decrypt garbage silently.
pub fn decrypt(blob: &[u8]) -> Result<Vec<u8>, String> {
    if blob.len() < 12 {
        return Err("ciphertext too short to contain a nonce".to_string());
    }
    let (nonce_bytes, ciphertext) = blob.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    cipher()
        .decrypt(nonce, ciphertext)
        .map_err(|_| "decryption failed — wrong ENOCHIAN_MASTER_KEY, or data is corrupted/tampered".to_string())
}

/// Minimal hex codec, written by hand rather than pulling in the `hex`
/// crate for two lines of logic — one less dependency in a codebase
/// that's already hit real friction from crate version drift this
/// project (see SECURITY_NOTES.md's toolchain notes).
pub fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err("hex string has an odd number of characters".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| format!("invalid hex at position {i}: {e}"))
        })
        .collect()
}

#[cfg(test)]
mod key_posture_tests {
    //! What counts as a development key. `cipher` already panics on a
    //! malformed one — at the first write, as an unhandled panic — so
    //! the point of recognising it here is that it arrives as a
    //! diagnosis at start-up instead.
    use super::*;

    #[test]
    fn an_unset_key_is_a_defect() {
        assert_eq!(key_defect_in(None), Some(KeyDefect::NotConfigured));
    }

    /// Spelling the development key out longhand is the same key. The
    /// check is on the bytes, not on the text, so whitespace and case
    /// cannot get around it.
    #[test]
    fn the_all_zero_key_is_a_defect_however_it_is_written() {
        for raw in [DEV_KEY_HEX, &format!("  {DEV_KEY_HEX}  "), &DEV_KEY_HEX.to_uppercase()] {
            assert_eq!(
                key_defect_in(Some(raw)),
                Some(KeyDefect::AllZero),
                "{raw:?} should have been recognised as the dev key"
            );
        }
    }

    #[test]
    fn a_malformed_key_is_a_defect_at_startup_not_a_panic_at_first_write() {
        assert!(matches!(
            key_defect_in(Some("not hex at all")),
            Some(KeyDefect::Malformed(_))
        ));

        // Right alphabet, wrong length.
        assert!(matches!(
            key_defect_in(Some("aabb")),
            Some(KeyDefect::Malformed(_))
        ));
    }

    #[test]
    fn a_real_key_is_not_a_defect() {
        let key = "0".repeat(63) + "1";

        assert_eq!(key_defect_in(Some(&key)), None);
    }
}

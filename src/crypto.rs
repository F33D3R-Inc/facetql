use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::aead::consts::U12;
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use std::sync::OnceLock;

/// An obviously-fake, all-zero key — used only when FACETQL_MASTER_KEY
/// isn't set, so local development doesn't need any setup. Printed as a
/// loud warning every time it's used, same pattern as the dev API token
/// in auth.rs. Anything encrypted with this key is not secure — it's a
/// known, public value.
const DEV_KEY_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn cipher() -> &'static Aes256Gcm {
    static CIPHER: OnceLock<Aes256Gcm> = OnceLock::new();
    CIPHER.get_or_init(|| {
        let key_hex = std::env::var("FACETQL_MASTER_KEY").unwrap_or_else(|_| {
            eprintln!(
                "warning: FACETQL_MASTER_KEY not set — using an all-zero dev key. \
                 Data encrypted with this key is NOT secure. Generate a real key with \
                 `openssl rand -hex 32` and set FACETQL_MASTER_KEY to it before running \
                 with real data."
            );
            DEV_KEY_HEX[..64].to_string()
        });

        let key_bytes = decode_hex(&key_hex).unwrap_or_else(|e| {
            panic!("FACETQL_MASTER_KEY is not valid hex: {e}")
        });

        if key_bytes.len() != 32 {
            panic!(
                "FACETQL_MASTER_KEY must decode to exactly 32 bytes (64 hex characters) \
                 for AES-256 — got {} bytes. Generate one with `openssl rand -hex 32`.",
                key_bytes.len()
            );
        }

        Aes256Gcm::new_from_slice(&key_bytes).expect("key is exactly 32 bytes, checked above")
    })
}

/// Encrypts `plaintext` with AES-256-GCM under a fresh random 12-byte
/// nonce, and returns `nonce || ciphertext` as one blob.
pub fn encrypt(plaintext: &[u8]) -> Vec<u8> {
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);

    // FOOLPROOF FIX: Nonce<U12> implements From<[u8; 12]>. No deprecation, no generics needed.
    let nonce = Nonce::<U12>::from(nonce_bytes);

    let ciphertext = cipher()
        .encrypt(&nonce, plaintext)
        .expect("AES-GCM encryption failed");

    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    out
}

/// Reverses `encrypt`. Fails if the blob is too short, wrong key, or tampered.
pub fn decrypt(blob: &[u8]) -> Result<Vec<u8>, String> {
    if blob.len() < 12 {
        return Err("ciphertext too short to contain a nonce".to_string());
    }
    let (nonce_bytes, ciphertext) = blob.split_at(12);

    // FOOLPROOF FIX: Copy the slice into a fixed-size array, then use From.
    let mut nonce_array = [0u8; 12];
    nonce_array.copy_from_slice(nonce_bytes);
    let nonce = Nonce::<U12>::from(nonce_array);

    cipher()
        .decrypt(&nonce, ciphertext)
        .map_err(|_| "decryption failed — wrong FACETQL_MASTER_KEY, or data is corrupted/tampered".to_string())
}

/// Minimal hex codec.
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
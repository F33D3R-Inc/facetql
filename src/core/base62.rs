const SYMBOLS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub fn encode(value: u8) -> char {
    SYMBOLS[value as usize] as char
}

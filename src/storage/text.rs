//! The inverted index: an access path for **substring search**.
//!
//! ```text
//!           logical question                       index
//!   ------------------------------------------  -----------
//!   which nodes' text contains this substring?   text (here)
//! ```
//!
//! Every other access path in [`crate::storage::index`] answers a
//! question a *sorted* structure can answer — a point, a prefix, a
//! range. `contains` is not one of those. No range of a B+tree over
//! whole values corresponds to "this text appears somewhere inside", so
//! before this module a search query read every node of its kind, decoded
//! its JSON and ran `str::contains` on it. That is a scan, and a website
//! built on this engine needs search on the *first* page of results, not
//! after the scan bound refuses.
//!
//! # What is indexed: byte trigrams, not words
//!
//! The critical decision here is what a token *is*, and it is forced by
//! one requirement: **the planner must never return a different answer
//! than the scan would**.
//!
//! A word index cannot meet that. `contains("ell")` matches `"hello"`
//! under `str::contains` and does not match any word of it, so a word
//! index would silently drop rows — a wrong answer that looks like a
//! valid empty page, which is the worst outcome a query engine has. So
//! the unit here is the **3-byte window** (trigram) of the text, which
//! genuinely corresponds to substrings:
//!
//! ```text
//!   "hello"  →  hel  ell  llo
//!   needle "ell"  →  ell           ⊂ the postings of "hello"
//! ```
//!
//! The property that makes this sound is one-directional and that is
//! exactly enough:
//!
//! > if `v.contains(s)` then every trigram of `s` is a trigram of `v`.
//!
//! The converse is false — `"ell"` and `"llo"` can occur in a text that
//! never contains `"ello"` — so the intersection of the needle's posting
//! lists is a **superset** of the true answer, never a subset. The index
//! is therefore a *candidate generator*, and the exact predicate is still
//! evaluated on every candidate by the same `matches` closure every other
//! access path funnels through. The index changes how many rows are read;
//! it never decides which ones match. That is what lets it serve
//! `contains`, `starts_with` and `ends_with` alike — all three are
//! substring tests — with the answer pinned to the scan's by
//! construction.
//!
//! # Case folding
//!
//! Both sides — the indexed text and the probed needle — are ASCII-
//! lowercased, bytewise. Two consequences, both deliberate:
//!
//! * folding only ever *widens* the candidate set relative to the
//!   case-sensitive test the evaluator performs, so the superset
//!   property above survives it untouched; and
//! * the same postings already serve a case-insensitive search, which is
//!   what an application's search box actually wants.
//!
//! Non-ASCII bytes are left exactly as they are. Full Unicode folding is
//! not length-preserving (`İ` folds to two code points), and the whole
//! argument above rests on the fold being a *bytewise, length-preserving*
//! map: if it were not, a substring of the raw text would not be a
//! substring of the folded text and the superset claim would fail. A
//! search that has to fold non-ASCII correctly is a different index, not
//! a looser version of this one.
//!
//! # No positions, no stemming, no stopwords
//!
//! Positions would let a phrase be verified inside the index, but a
//! phrase query *is* a substring query — `contains("quick brown")` —
//! and its trigram set already spans the space between the words, so the
//! candidate set is the same one. Positions would only move the recheck,
//! and the recheck is not optional anyway.
//!
//! Stemming and stopword removal are absent for the reason a word index
//! is: each of them makes the index's answer differ from the scan's.
//!
//! # The discipline this depends on
//!
//! Identical to [`crate::storage::index`]: this is a derived structure,
//! maintained only by `StorageEngine::apply_committed`, so a posting the
//! index keeps after the row that produced it is gone is not a slow
//! query — it is a deleted row resurrected into a search result. Every
//! mutation path retracts the *previous* value's postings before
//! asserting the new one's.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config;
use crate::storage::btree::{BTree, MAX_KEY_LEN};
use crate::storage::index::{check_component, MAX_INDEX_NAME_LEN};

/// Width of one indexed window, in bytes.
///
/// Three is the standard choice and the balance is the reason: at two,
/// the posting lists of common pairs are most of the kind and the index
/// stops narrowing anything; at four, a needle has to be four bytes long
/// before the index can serve it at all. Three keeps every needle of
/// three bytes or more indexable while leaving 16.7 million distinct
/// windows to spread the postings across.
pub const GRAM_LEN: usize = 3;

/// Longest text this index will accept in one field of one node.
///
/// This is a bound on *write amplification*, not on storage: a value of
/// length L produces up to L−2 postings, and every one of them is a
/// separate B+tree insert on the write path and a separate removal on
/// the next update. An unbounded field would therefore let one request
/// impose an unbounded number of index writes — and, worse, would do it
/// *after* the WAL has the mutation, where the write can no longer be
/// refused.
///
/// 16 KiB is far past any body of text a search box is pointed at and
/// still bounds one write at a few thousand postings. A field that
/// genuinely holds more than this is a document, and a document belongs
/// behind an index built for documents.
pub const MAX_TEXT_VALUE_LEN: usize = 16 * 1024;

/// One operator-declared inverted index over a `data` field.
pub struct TextIndex {
    pub def: TextIndexDef,
    pub tree: BTree,
}

/// The declaration of an inverted index.
///
/// Deliberately *not* a variant of [`crate::storage::index::IndexDef`]
/// with a mode field. The definition logs are bincode, which is
/// positional and not self-describing, so adding a field to a logged
/// struct makes every record an existing database already wrote
/// unreadable. A separate definition in a separate log is the same shape
/// [`crate::storage::reference`] already takes, and it costs an existing
/// deployment nothing.
///
/// There is no `unique` here and there will not be. Uniqueness is a
/// statement about a whole value; this index does not store whole values,
/// it stores windows of them, so it has nothing to be unique about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextIndexDef {
    /// Operator-chosen identity, unique across *both* index kinds —
    /// `DELETE /admin/indexes/:name` names one index, so two indexes
    /// cannot share a name and still be droppable. Also the index's
    /// filename suffix, which is why the alphabet is restricted.
    pub name: String,

    /// The node `kind` this index covers, per-kind for the same reason
    /// an ordered index is: `data` has no schema across kinds.
    pub kind: String,

    /// The top-level `data` field whose text is indexed. Only a JSON
    /// string is indexed — every other type answers `false` to all three
    /// substring tests in the evaluator, so a node carrying one has no
    /// postings and is correctly absent from every candidate set.
    pub field: String,
}

/// One operation in `facetql.text_indexes`.
///
/// Same shape and the same last-write-wins replay as
/// [`crate::storage::index::IndexOpRecord`] — file order in one log is
/// the total order for the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TextIndexOpRecord {
    Put(TextIndexDef),
    Drop(String),
}

impl TextIndexDef {
    /// Reject a definition the storage layer could not honour, while
    /// rejecting it is still just a failed request.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name.len() > MAX_INDEX_NAME_LEN {
            return Err(format!(
                "index name must be 1..={MAX_INDEX_NAME_LEN} bytes"
            ));
        }

        // The name is interpolated into a filename.
        if !self
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(
                "index name may contain only letters, digits, '_' and '-'"
                    .to_string(),
            );
        }

        if self.kind.is_empty() {
            return Err("index kind must not be empty".to_string());
        }

        if self.field.is_empty() {
            return Err("index field must not be empty".to_string());
        }

        check_component("index kind", &self.kind)?;
        check_component("index field", &self.field)?;

        Ok(())
    }
}

/// The index-definition log — see [`TextIndexOpRecord`].
pub fn definitions_path() -> PathBuf {
    config::data_file("facetql.text_indexes")
}

/// The tree backing one declared inverted index.
///
/// A separate filename namespace from `facetql.idx.data.<name>`, so the
/// two kinds of index can never open each other's file even if a name
/// somehow collided.
pub fn index_path(name: &str) -> PathBuf {
    config::data_file(&format!("facetql.idx.text.{name}"))
}

// ---------------------------------------------------------------------
// Tokenization
// ---------------------------------------------------------------------

/// The bytewise, length-preserving fold applied to both sides.
///
/// ASCII only, on purpose — see the module docs. `u8::to_ascii_lowercase`
/// leaves every byte ≥ 0x80 alone, which is every byte of a multi-byte
/// UTF-8 sequence, so a folded string is still valid UTF-8 and still has
/// exactly the same length and the same byte offsets as the original.
fn fold_into(text: &str, out: &mut Vec<u8>) {
    out.clear();
    out.extend(text.as_bytes().iter().map(u8::to_ascii_lowercase));
}

/// Every distinct trigram of `text`, sorted.
///
/// Sorted and deduplicated because both consumers want that: the write
/// path writes each posting once, and the probe path wants a
/// deterministic order so two identical queries plan identically.
pub fn grams(text: &str) -> Vec<[u8; GRAM_LEN]> {
    let mut folded = Vec::with_capacity(text.len());
    fold_into(text, &mut folded);

    let mut set: BTreeSet<[u8; GRAM_LEN]> = BTreeSet::new();

    for window in folded.windows(GRAM_LEN) {
        // `windows` yields exactly GRAM_LEN bytes, so this cannot fail.
        let mut gram = [0u8; GRAM_LEN];
        gram.copy_from_slice(window);
        set.insert(gram);
    }

    set.into_iter().collect()
}

/// The string this index would store for a node, if any.
///
/// `None` for an absent field and for every non-string type, matching
/// the evaluator: `contains`/`starts_with`/`ends_with` all answer
/// `false` when either side is not a string, so a node with a numeric
/// `title` matches no substring test and must have no postings.
pub fn indexed_text<'a>(data: Option<&'a Value>, field: &str) -> Option<&'a str> {
    data?.get(field)?.as_str()
}

// ---------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------

/// `gram · address → ()`.
///
/// Membership only, like the `kind` and `owner` indexes: the record's
/// location comes from the primary index, so a node moving in the heap —
/// which every update does — costs nothing here.
///
/// The gram needs no length prefix because it is fixed-width: three
/// bytes, always, so the address can never be mistaken for part of it and
/// no address can spoof the start of another gram's range. That is the
/// same property [`crate::storage::index::component`] buys for
/// variable-length components, obtained here for free.
pub fn key(gram: &[u8; GRAM_LEN], address: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(GRAM_LEN + address.len());
    out.extend_from_slice(gram);
    out.extend_from_slice(address.as_bytes());
    out
}

/// Recover the address from a posting key.
pub fn address_from_key(key: &[u8]) -> Option<&[u8]> {
    if key.len() <= GRAM_LEN {
        return None;
    }

    Some(&key[GRAM_LEN..])
}

/// The postings a node would produce in every inverted index declared
/// over its kind, checked as a set.
///
/// Runs **before the WAL**, for the reason every other admissibility
/// check does: past the WAL a refusal is not a rejected request, it is a
/// committed mutation that recovery replays into the same refusal on
/// every subsequent start.
pub fn check_text_keys<'a>(
    defs: impl Iterator<Item = &'a TextIndexDef>,
    address: &str,
    data: &str,
) -> Result<(), String> {
    let mut decoded: Option<Value> = None;
    let mut parsed = false;

    for def in defs {
        if !parsed {
            decoded = serde_json::from_str(data).ok();
            parsed = true;
        }

        let Some(text) = indexed_text(decoded.as_ref(), &def.field) else {
            continue;
        };

        if text.len() > MAX_TEXT_VALUE_LEN {
            return Err(format!(
                "field '{}' holds {} bytes of text, over the \
                 {MAX_TEXT_VALUE_LEN}-byte maximum for the inverted index \
                 '{}'. Every byte is a posting, so the bound is a bound on \
                 how much index writing one request may impose.",
                def.field,
                text.len(),
                def.name
            ));
        }

        // Every posting for this node has the same length, so one
        // representative gram stands in for all of them.
        if GRAM_LEN + address.len() > MAX_KEY_LEN {
            return Err(format!(
                "the '{}' index key would be {} bytes; the maximum is \
                 {MAX_KEY_LEN}",
                def.name,
                GRAM_LEN + address.len()
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one property the planner's correctness rests on: a substring's
    /// trigrams are a subset of the haystack's, so an intersection of
    /// posting lists can only ever be a superset of the true matches.
    #[test]
    fn a_substrings_grams_are_a_subset_of_the_haystacks() {
        let haystack = "The Quick Brown Fox";

        for start in 0..haystack.len() {
            for end in (start + GRAM_LEN)..=haystack.len() {
                let needle = &haystack[start..end];

                for gram in grams(needle) {
                    assert!(
                        grams(haystack).contains(&gram),
                        "gram {:?} of needle {needle:?} is missing from the \
                         haystack's grams",
                        String::from_utf8_lossy(&gram),
                    );
                }
            }
        }
    }

    /// Folding is bytewise and length-preserving, which is what makes the
    /// subset property survive it.
    #[test]
    fn folding_is_case_insensitive_and_keeps_length() {
        assert_eq!(grams("ABC"), grams("abc"));
        assert_eq!(grams("AbC"), grams("aBc"));

        let mut folded = Vec::new();
        fold_into("Ünïcode ÅBC", &mut folded);
        assert_eq!(folded.len(), "Ünïcode ÅBC".len());
        assert_eq!(
            String::from_utf8(folded).expect("still utf-8"),
            "Ünïcode Åbc",
        );
    }

    /// A needle shorter than one window produces no grams at all, which
    /// is how the planner learns it has to fall back to the scan.
    #[test]
    fn a_needle_under_one_window_has_no_grams() {
        assert!(grams("").is_empty());
        assert!(grams("a").is_empty());
        assert!(grams("ab").is_empty());
        assert_eq!(grams("abc").len(), 1);
    }

    #[test]
    fn a_posting_key_round_trips_its_address() {
        let gram = *b"abc";
        let encoded = key(&gram, "Post:0001");

        assert!(encoded.starts_with(&gram));
        assert_eq!(address_from_key(&encoded), Some(&b"Post:0001"[..]));

        // A key with nothing after the gram is not one this encoding
        // produces: an address is never empty.
        assert_eq!(address_from_key(&gram[..]), None);
    }

    #[test]
    fn only_a_string_field_is_indexed() {
        let data: Value = serde_json::json!({
            "body": "hello",
            "score": 5,
            "tags": ["a"],
        });

        assert_eq!(indexed_text(Some(&data), "body"), Some("hello"));
        assert_eq!(indexed_text(Some(&data), "score"), None);
        assert_eq!(indexed_text(Some(&data), "tags"), None);
        assert_eq!(indexed_text(Some(&data), "absent"), None);
        assert_eq!(indexed_text(None, "body"), None);
    }
}

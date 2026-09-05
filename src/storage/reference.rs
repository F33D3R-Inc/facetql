//! Declared referential integrity: which `data` field of which kind
//! points at which other kind, and what a delete of the referenced node
//! does to the nodes referencing it.
//!
//! # Why this belongs in the engine
//!
//! Nothing here is new information — an application that stores
//! `{"post": "Post:12"}` on a `Comment` already knows that comment
//! belongs to that post. What it cannot do from outside is act on it
//! *atomically*, or *cheaply*, or *at all* without holding the data:
//!
//! * deleting a post and its comments as two requests is two
//!   transactions, and a crash between them leaves comments whose post
//!   is gone — rows nothing can reach and nothing will ever clean up;
//! * finding those comments without an access path is a scan of every
//!   comment in the database per delete;
//! * so the caller keeps a copy of the graph in memory to avoid the
//!   scan, and now the size of the application is the size of the
//!   database.
//!
//! That third consequence is the one that actually bites: it is the
//! reason a runtime loads every row at boot. A referential action
//! declared here is one durable rule the engine enforces inside the
//! same frame as the delete that triggered it, using the index that
//! already exists.
//!
//! # What a declaration requires, and why
//!
//! A reference is only accepted when the access paths that make it
//! cheap in *both* directions already exist:
//!
//! * an index over the referencing `(kind, field)` — without it, every
//!   delete of a referenced node is a full scan of the referencing
//!   kind. This is the same identification the unique constraint makes:
//!   a rule with no access path behind it is a rule that costs a scan.
//! * a **unique** index over the referenced `(parent_kind,
//!   parent_field)`, when the reference is by a data field rather than
//!   by address. A reference has to name exactly one node; a value that
//!   two nodes can hold names neither. SQL requires the same thing, and
//!   for the same reason. Referencing by address needs nothing declared
//!   — an address is unique by construction and the primary index is
//!   always there.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::config;

/// What a delete of the referenced node does to the nodes referencing
/// it.
///
/// The three SQL actions that are decidable from the reference alone.
/// `SET DEFAULT` is deliberately absent: `data` is schemaless, so there
/// is no declared default to set a field back to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferentialAction {
    /// Delete the referencing nodes too, and whatever references *them*
    /// — the transitive closure, in the same frame.
    Cascade,

    /// Refuse the delete while anything still references the node.
    ///
    /// Immediate, like SQL's `RESTRICT` and unlike its `NO ACTION`: the
    /// check runs where the delete is lowered, not at the end of the
    /// batch. A batch that deletes the children first and the parent
    /// second is therefore accepted; one that deletes the parent first
    /// is refused, and the caller reorders it.
    Restrict,

    /// Set the referencing field to `null`, leaving the nodes.
    ///
    /// An absent or null reference is not a reference, so the result
    /// satisfies the rule the same way a deleted child does.
    SetNull,
}

/// One declared reference.
///
/// Named after the *referencing* side (`kind`.`field`), because that is
/// the side that carries the value and the side whose index serves it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceDef {
    /// Operator-chosen identity, used to drop it again. Restricted to
    /// the same alphabet as an index name because it is also a URL path
    /// segment.
    pub name: String,

    /// The kind that *holds* the reference — the child.
    pub kind: String,

    /// The top-level `data` field on that kind holding the referenced
    /// node's key. Null or absent means "references nothing", which is
    /// always admissible; that is SQL's nullable foreign key.
    pub field: String,

    /// The kind being *referenced* — the parent.
    ///
    /// Checked on resolution rather than assumed: a value that resolves
    /// to a node of some other kind is a dangling reference that
    /// happens to collide, and treating it as valid would make the
    /// cascade delete an unrelated node's children.
    pub parent_kind: String,

    /// Which value on the parent the child's field matches.
    ///
    /// `None` — the default — means the parent's **address**, which is
    /// the engine's own identity and needs no declared index to resolve.
    /// `Some(field)` means a top-level `data` field of the parent, which
    /// must carry a unique index; this is the shape an application has
    /// when its own integer ids live in `data` and the address is
    /// derived from them.
    #[serde(default)]
    pub parent_field: Option<String>,

    /// What deleting the parent does to its children.
    pub on_delete: ReferentialAction,
}

/// Longest a reference name may be. Same bound as an index name, for
/// the same reason: it travels as a URL path segment.
pub const MAX_REFERENCE_NAME_LEN: usize = 64;

impl ReferenceDef {
    /// Reject a definition the engine could not honour, while rejecting
    /// it is still just a failed request.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name.len() > MAX_REFERENCE_NAME_LEN {
            return Err(format!(
                "reference name must be 1..={MAX_REFERENCE_NAME_LEN} bytes"
            ));
        }

        if !self
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(
                "reference name may contain only letters, digits, '_' and '-'"
                    .to_string(),
            );
        }

        for (label, value) in [
            ("reference kind", &self.kind),
            ("reference field", &self.field),
            ("reference parent_kind", &self.parent_kind),
        ] {
            if value.is_empty() {
                return Err(format!("{label} must not be empty"));
            }

            crate::storage::index::check_component(label, value)?;
        }

        if let Some(parent_field) = &self.parent_field {
            if parent_field.is_empty() {
                return Err(
                    "reference parent_field must not be empty when present"
                        .to_string(),
                );
            }

            crate::storage::index::check_component(
                "reference parent_field",
                parent_field,
            )?;
        }

        // A node referencing its own kind is a tree (a reply to a
        // reply), which is supported and cascades transitively. A node
        // referencing *itself* through a field is not: the cascade would
        // have to delete the node it is already deleting, and a
        // `restrict` would refuse every delete forever.
        if self.kind == self.parent_kind
            && self.parent_field.as_deref() == Some(self.field.as_str())
        {
            return Err(format!(
                "reference {:?} points {}.{} at itself",
                self.name, self.kind, self.field
            ));
        }

        Ok(())
    }
}

/// One operation in `facetql.references`, the reference-definition log.
///
/// Same shape and the same last-write-wins replay as the index and user
/// logs: file order in one log is the total order for the key, so a
/// reference can be declared, dropped and declared again with nothing to
/// reconcile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReferenceOpRecord {
    Put(ReferenceDef),
    Drop(String),
}

/// The declared references, resident and consulted on every write.
///
/// Bounded by how many an operator declared rather than by the amount of
/// data, exactly like the index definitions — which is what makes it
/// affordable to keep the whole set in memory and answer "does this
/// mutation touch a reference" without a read.
#[derive(Default)]
pub struct References {
    defs: RwLock<HashMap<String, ReferenceDef>>,
}

impl References {
    pub fn new() -> References {
        References::default()
    }

    /// Declare (or re-declare) one reference. Idempotent by name, so
    /// replaying a `CreateReference` record is harmless.
    pub fn put(&self, def: ReferenceDef) {
        self.write().insert(def.name.clone(), def);
    }

    /// Forget one. Dropping one that is not there is not an error:
    /// recovery replays a drop against a database that already applied
    /// it.
    pub fn remove(&self, name: &str) {
        self.write().remove(name);
    }

    pub fn get(&self, name: &str) -> Option<ReferenceDef> {
        self.read().get(name).cloned()
    }

    /// Every declared reference, in name order so listings and error
    /// messages are stable.
    pub fn all(&self) -> Vec<ReferenceDef> {
        let mut all: Vec<ReferenceDef> = self.read().values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// The references a node of this kind carries — what an insert of it
    /// has to resolve.
    pub fn for_child(&self, kind: &str) -> Vec<ReferenceDef> {
        self.filtered(|def| def.kind == kind)
    }

    /// The references pointing *at* this kind — what a delete of one of
    /// its nodes has to act on.
    pub fn for_parent(&self, kind: &str) -> Vec<ReferenceDef> {
        self.filtered(|def| def.parent_kind == kind)
    }

    /// Every reference that depends on the index over `(kind, field)`.
    ///
    /// Both sides count: the child index is how a cascade finds its
    /// targets, and the parent index is what makes the referenced value
    /// unique. Dropping either leaves a rule with no way to be enforced.
    pub fn depending_on_index(&self, kind: &str, field: &str) -> Vec<ReferenceDef> {
        self.filtered(|def| {
            (def.kind == kind && def.field == field)
                || (def.parent_kind == kind
                    && def.parent_field.as_deref() == Some(field))
        })
    }

    fn filtered(&self, keep: impl Fn(&ReferenceDef) -> bool) -> Vec<ReferenceDef> {
        let mut all: Vec<ReferenceDef> =
            self.read().values().filter(|d| keep(d)).cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, ReferenceDef>> {
        self.defs.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, ReferenceDef>> {
        self.defs.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// The reference-definition log — see [`ReferenceOpRecord`].
pub fn definitions_path() -> PathBuf {
    config::data_file("facetql.references")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str) -> ReferenceDef {
        ReferenceDef {
            name: name.to_string(),
            kind: "Comment".to_string(),
            field: "post".to_string(),
            parent_kind: "Post".to_string(),
            parent_field: None,
            on_delete: ReferentialAction::Cascade,
        }
    }

    #[test]
    fn a_name_that_could_be_a_path_is_refused() {
        let mut d = def("../escape");
        assert!(d.validate().is_err());

        d.name = "post_comments".to_string();
        assert!(d.validate().is_ok());
    }

    #[test]
    fn a_field_pointing_at_itself_is_refused() {
        let mut d = def("self");
        d.parent_kind = "Comment".to_string();
        d.parent_field = Some("post".to_string());

        assert!(d.validate().is_err(), "a field cannot reference itself");

        // The same kind through a *different* field is a tree, which is
        // exactly what a reply-to-a-reply is.
        d.field = "parent".to_string();
        assert!(d.validate().is_ok());
    }

    #[test]
    fn lookups_are_by_side() {
        let refs = References::new();
        refs.put(def("a"));

        assert_eq!(refs.for_child("Comment").len(), 1);
        assert_eq!(refs.for_child("Post").len(), 0);
        assert_eq!(refs.for_parent("Post").len(), 1);
        assert_eq!(refs.for_parent("Comment").len(), 0);

        refs.remove("a");
        assert!(refs.is_empty());
    }

    #[test]
    fn an_index_both_sides_need_is_found_from_either_side() {
        let refs = References::new();
        let mut d = def("a");
        d.parent_field = Some("id".to_string());
        refs.put(d);

        assert_eq!(refs.depending_on_index("Comment", "post").len(), 1);
        assert_eq!(refs.depending_on_index("Post", "id").len(), 1);
        assert_eq!(refs.depending_on_index("Post", "body").len(), 0);
    }
}

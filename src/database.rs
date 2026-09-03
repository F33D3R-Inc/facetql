use std::io;
use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;

use crate::config;
use crate::storage::engine::StorageEngine;
use crate::storage::recovery;

/// Who is allowed to receive a live event.
///
/// Every event carries one of these because the notification channel is
/// a *read path*, and a read path without visibility rules is a leak: a
/// node the reader could never fetch over `GET /node/:address` must not
/// announce itself over `GET /events` either. Making the audience part
/// of the event (rather than something the subscriber tries to
/// reconstruct) means the decision is taken where the facts are — at the
/// handler that just performed the write and knows whose data it was.
#[derive(Clone, Debug)]
pub enum Audience {
    /// Any authenticated subscriber. For events that concern public
    /// nodes, and for explicit application broadcasts.
    Everyone,

    /// Only this owner — plus admins, who can already read everything.
    /// The default shape for anything touching a private node.
    Owner(String),
}

impl Audience {
    /// May a subscriber authenticated as `owner` receive this?
    pub fn admits(&self, owner: &str, is_admin: bool) -> bool {
        match self {
            Audience::Everyone => true,
            Audience::Owner(only) => is_admin || only == owner,
        }
    }

    /// The audience for an event about a node: public nodes are
    /// announced to everyone, private nodes only to their owner. This is
    /// the same rule `Node::can_read` applies to a direct fetch, which is
    /// the point — one visibility model, not two that can drift apart.
    pub fn for_node(node: &crate::core::node::Node) -> Audience {
        match node.visibility {
            crate::core::node::Visibility::Public => Audience::Everyone,
            crate::core::node::Visibility::Private => {
                Audience::Owner(node.owner.clone())
            }
        }
    }
}

/// A live notification and the audience permitted to see it.
#[derive(Clone, Debug)]
pub struct LiveEvent {
    pub payload: String,
    pub audience: Audience,
}

#[derive(Clone)]
pub struct Database {
    pub engine: Arc<RwLock<StorageEngine>>,
    pub broadcaster: broadcast::Sender<LiveEvent>,
}

impl Database {
    pub fn new() -> io::Result<Self> {
        config::ensure_data_dir()?;

        let mut engine = StorageEngine::load()?;

        /*
         * WAL recovery is part of opening the database.
         *
         * A recovery failure must prevent startup. Continuing after
         * an authentication, corruption, or format error could cause
         * the server to expose state that is not known to be durable
         * or valid.
         */
        recovery::recover(&mut engine)?;

        let (broadcaster, _) =
            broadcast::channel(1024);

        Ok(Self {
            engine: Arc::new(RwLock::new(engine)),
            broadcaster,
        })
    }

    /// Publish a database event to the subscribers `audience` admits.
    ///
    /// The database mutation itself is responsible for durability.
    /// This channel is only the live notification mechanism and must
    /// never be treated as the source of truth.
    ///
    /// The audience is a required argument rather than something with a
    /// default: a new publish site should have to say who may see its
    /// event, because the failure mode of getting it wrong is silent
    /// disclosure, and a default would be chosen by whoever was in a
    /// hurry.
    pub fn publish(&self, audience: Audience, payload: String) {
        /*
         * There may be no subscribers. broadcast::Sender::send()
         * returns an error in that case, but that does not mean the
         * database operation failed.
         *
         * Events are intentionally best-effort notifications.
         */
        let _ = self.broadcaster.send(LiveEvent { payload, audience });
    }
}
#[cfg(test)]
mod audience_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};

    fn node(owner: &str, visibility: Visibility) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            "aud:1".to_string(),
            "Thing".to_string(),
            owner.to_string(),
        );
        n.visibility = visibility;
        n
    }

    /// The rule that closes the `/events` leak: an event about a private
    /// node reaches its owner and admins, and nobody else.
    #[test]
    fn private_node_events_do_not_reach_other_identities() {
        let audience = Audience::for_node(&node("alice", Visibility::Private));

        assert!(audience.admits("alice", false), "the owner sees its own node");
        assert!(audience.admits("root", true), "an admin reads everything already");
        assert!(
            !audience.admits("bob", false),
            "another identity must not learn a private node exists"
        );
    }

    /// A public node is public on every read path, the event stream
    /// included — otherwise a feed could never be built from it.
    #[test]
    fn public_node_events_reach_everyone() {
        let audience = Audience::for_node(&node("alice", Visibility::Public));

        assert!(audience.admits("alice", false));
        assert!(audience.admits("bob", false));
        assert!(audience.admits("root", true));
    }

    /// An explicit application broadcast is addressed to everyone by
    /// construction — the caller chose the payload.
    #[test]
    fn explicit_broadcasts_reach_everyone() {
        assert!(Audience::Everyone.admits("anyone", false));
    }
}

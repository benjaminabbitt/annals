//! What the kernel knows of a domain's types: its state and how its events
//! fold into it.

use angzarr_diagnostic::Diagnostic;
use angzarr_proto::v1::{ConflictResolution, StateDescriptor};

use super::conflict::Units;
use super::payload::Payload;

/// A domain's state: a domain type, with a prost message as its snapshot
/// form ([`State::Snapshot`]), so it can be snapshotted as `Any`. angzarr's
/// generator (`angzarr_build`, backlog CR-1 and CR-4) emits the impl from the
/// state message, its `(io.angzarr.v1.state)` option and its
/// `(io.angzarr.v1.conflict)` field markers; a state that is itself a prost
/// message is its own snapshot form.
pub trait State: Default + Clone + Send + 'static {
    /// The wire form a snapshot carries. Its type URL is the state's, as
    /// [`State::descriptor`] names it; registration refuses a descriptor
    /// naming another.
    type Snapshot: prost::Message + prost::Name + Default;

    /// The state as its snapshot form. Infallible: every state has one.
    fn to_snapshot(&self) -> Self::Snapshot;

    /// The state a snapshot holds, or why it holds none: a wire value need
    /// not be a valid state (a required field absent, an enum value the
    /// domain does not define, a broken invariant). A refusal makes the
    /// snapshot unreadable ([`PayloadError::Invalid`](super::PayloadError)).
    fn from_snapshot(snapshot: Self::Snapshot) -> Result<Self, angzarr_diagnostic::Report>;

    /// Overwrite this instance's state with what `snapshot` holds, keeping
    /// whatever the snapshot does not (a marked-up aggregate's immutable
    /// configuration, from the registry's factory). Resuming builds a fresh
    /// instance, then restores it. The default replaces the whole instance
    /// with [`State::from_snapshot`]'s.
    fn restore(&mut self, snapshot: Self::Snapshot) -> Result<(), angzarr_diagnostic::Report> {
        *self = Self::from_snapshot(snapshot)?;
        Ok(())
    }

    /// The state's type URL and the conflict resolutions it supports, the
    /// full resolved list (`[COMMUTATIVE, STRICT, AGGREGATE_HANDLES]` for a
    /// state that lists COMMUTATIVE and marks its units; `[STRICT,
    /// AGGREGATE_HANDLES]`, the implicit base, for one with no option). What
    /// [`Registry::register`](super::Registry::register) records, as
    /// angzarr's `ComponentDescriptor.state`.
    fn descriptor() -> StateDescriptor;

    /// Whether the kernel takes snapshots of this state. `false` for a
    /// state with no snapshot form (a marked-up aggregate without
    /// `snapshot = ..`, whose [`State::Snapshot`] is
    /// [`NoSnapshot`](super::markup::NoSnapshot)): its streams are always
    /// replayed from the start, and [`Stream::snapshot`](super::Stream::snapshot)
    /// is `None`.
    const SNAPSHOTS: bool = true;

    /// The conflict units whose values differ between `before` and
    /// `after` (angzarr's `conflict_units` table, field kind by field kind,
    /// with the fail-safe wildcard for every unmarked field that differs).
    /// Pure. Called only under COMMUTATIVE, so only for a state that lists
    /// it; the default, for a state with no units, says "everything".
    fn changed(_before: &Self, _after: &Self) -> Units {
        Units::everything()
    }
}

/// One kind of event stream, as the kernel reads it: its state, its
/// commands and facts, its events and how each folds into the state. Its
/// handlers are angzarr-router's: a domain is dispatched by the router's
/// `DomainDispatch` over its state, built by the kernel from this
/// description ([`Registry::register`](super::Registry::register)), its
/// command and fact handlers registered on it (`#[aggregate]` and
/// `#[process_manager]` generate them).
///
/// - **Payloads are prost messages.** Commands, facts and events are
///   [`Payload`]s, packed as `Any` by type URL; the state is a [`State`].
/// - **Business output is payloads only.** A handler emits events; the
///   kernel builds the pages: their sequences and their `EventMeta`.
/// - everything `'static` and `Send`.
pub trait Domain: Sized + 'static {
    /// Folded from the events. Never stored, except as a snapshot.
    type State: State;
    /// A request to change the stream. Decided, never stored.
    type Command: Payload + Send + 'static;
    /// Something that already happened outside the system (a webhook, an
    /// integration). Recorded, never refused.
    /// [`std::convert::Infallible`] for a domain that takes none.
    type Fact: Payload + Send + 'static;
    /// What a stream stores.
    type Event: Payload + Clone + Send + Sync + 'static;
    /// Why its handlers refuse a command, for the person who gave it. The
    /// caller receives it as a [`Rejected`](super::Rejected): its code, args
    /// and typed refusal object. Kernel faults are never folded into it:
    /// see [`Failure`](super::Failure).
    type Rejection: Diagnostic + Send + 'static;
    /// The domain's name: the `Cover.domain` of every stream of this
    /// domain, and its key in the [`Registry`](super::Registry). Stable
    /// across releases; never contains `':'`.
    const NAME: &'static str;

    /// The full version of the build the domain is (types.proto,
    /// `EventMeta.rules_version`): its crate's `CARGO_PKG_VERSION`, the
    /// version angzarr-build writes into its descriptor, never a label
    /// chosen by hand (`#[aggregate]` and `#[process_manager]` give
    /// `env!("CARGO_PKG_VERSION")`). The domain is registered at it, and its
    /// answers report it; the kernel reads it only to choose which
    /// snapshots to offer ([`Registry::snapshot_usable`](super::Registry::snapshot_usable)).
    const RULES_VERSION: &'static str;

    /// The domain's configured conflict resolution (angzarr's component
    /// configuration). `None`: the built-in default, COMMUTATIVE where the
    /// state supports it, otherwise STRICT. Refused at registration if the
    /// state does not support it.
    const CONFLICT_RESOLUTION: Option<ConflictResolution> = None;

    /// Configured resolutions per command type, by the command message's
    /// full name (`"hatpick.list.v1.Draw"`): above the domain's own, below a
    /// book's request. Refused at registration if the state does not
    /// support one. Looked up by the command's current type, after
    /// upcasting: an old command type has its current type's setting, so
    /// only current types are named here.
    const CONFLICT_RESOLUTION_BY_COMMAND: &'static [(&'static str, ConflictResolution)] = &[];

    /// Fold one event into the state. Pure: the same state and event
    /// always give the same result. Never sees the event's metadata.
    fn apply(state: &mut Self::State, event: &Self::Event);
}

#[cfg(test)]
#[path = "domain.test.rs"]
mod tests;

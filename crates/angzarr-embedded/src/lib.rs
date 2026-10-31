//! A small, deterministic event-sourcing engine on angzarr's wire types:
//! angzarr's local runtime, designed in `docs/design/audit.md`: a domain
//! decides commands and ingests facts into events, applied to its folded
//! state, policies carry one domain's events into another's commands, and
//! every step is committed on its own through angzarr's shared store ports.
//!
//! The crate knows nothing about any particular application.
//!
//! Each module's tests are in its `*.test.rs`, over the toy domains of
//! `toys.test.rs`.
//!
//! angzarr-embedded is the in-process runtime, and it **operates on angzarr's wire
//! types directly** (`io.angzarr.v1`, re-exported as [`v1`]): a stream is a
//! [`Cover`](v1::Cover), a command a [`CommandBook`](v1::CommandBook), an
//! event an [`EventPage`](v1::EventPage) with its
//! [`EventMeta`](v1::EventMeta) (whose `causation` is the event's direct
//! cause), a trigger an [`EventRef`](v1::EventRef),
//! every id a 16-byte [`Uuid`](v1::Uuid), and derived ids are
//! [`angzarr_proto::ids`]'. There is no parallel type for any of them.
//! angzarr-embedded is **not** angzarr: where their execution models differ (the
//! atomic in-process chain, above all) the difference is deliberate and
//! listed in the design's *Divergences*.
//!
//! One command, end to end, in angzarr's normative order:
//!
//! 1. **Validate** ([`check`]): the one place a [`CommandBook`](v1::CommandBook)
//!    is checked, domain-blind, with the [`Registry`] for capabilities.
//!    What comes out is a [`Checked`], whose accessors are never optional
//!    where the book was required to say something. Nothing downstream
//!    unwraps a prost `Option`.
//! 2. **Identify** ([`Checked::identify`]): the command id (the client's,
//!    else the application's minted UUIDv7 from [`Sampled`]; a fact's
//!    UUIDv5 of its external id; a reaction's UUIDv5 of its trigger), the
//!    correlation (given, else the command id; a reaction's trigger's) and,
//!    for a reaction, the command's own causation (its trigger: transport
//!    and trace metadata, never stamped on its events). An [`Identified`].
//! 3. **Load, then admit**: the snapshot and newer events are loaded
//!    first; then the command id is looked up on the addressed stream, and
//!    the kernel admits the input (a refusal fact's address, deduplication
//!    by the events found, a process manager's start rule by its type after
//!    upcasting).
//!    Events found are answered [`Handled::Duplicate`], and nothing below
//!    runs. (Loading first closes the race in which a retry's lookup misses
//!    an original that commits before the retry loads.)
//! 4. **Resolve**: the expected sequence stamped or
//!    checked, against the head step 3 loaded, under the resolved
//!    [`ConflictResolution`](v1::ConflictResolution) (STRICT refuses a stale
//!    one here); the window and a nonce otherwise.
//! 5. **Dispatch**: angzarr-router's `CommandHandler` for the domain gets
//!    the `ContextualCommand` (the snapshot, the events, upcast, and the
//!    command, its handling resolved) and runs its handler: a command's,
//!    which may refuse ([`Rejected`]), or a fact's, which may not. The
//!    kernel awaits it; business code stays synchronous. Business
//!    output is payloads only: the kernel stamps every event's
//!    [`EventMeta`](v1::EventMeta).
//!
//! A domain whose streams another coordinator owns ([`Registry::remote`],
//! [`Target::Remote`]) takes steps 1 and 2 only: its command, identified,
//! is handed to that coordinator's [`CoordinatorPort`], and its answer is
//! the step's.
//!
//! Each step's output type is the next step's only input, so the order
//! cannot be skipped or reordered.
//!
//! Each step is ONE atomic write to the store (no unit of work): one
//! stream's events at the length it was loaded at, with the step's
//! positions. Then every [`Policy`] for the source's domain runs, through
//! angzarr-router's `PolicyComponent` (a `PolicyDispatch`, or a policy in
//! another process), on the written events, each of its commands a step of its own in its target,
//! depth first, with the policy's position written with it; a step that
//! fails keeps every write before it ([`Failure`], with the path to the
//! failing [`Step`]), and what is owed is resumed by position
//! ([`Kernel::resume`]). [`Projection`]s run on written events only. A policy command's trigger edge, which no event records, is the
//! command's own causation, for tracing.

// A failure names the path to the step that failed and carries reports, so
// it is large; failures are the cold path, and unboxed variants keep them
// matchable by pattern. Revisit (box `Downstream`) if profiling says so.
#![allow(clippy::result_large_err)]

mod check;
mod conflict;
mod coordinator;
mod dead_letter;
mod domain;
mod error;
mod fixture;
mod handle;
mod handler;
pub mod markup;
mod payload;
mod policy;
pub mod reasons;
mod registry;
mod route;
mod store;
mod stream;
mod target;
mod upcast;
mod wire;

// The toy domains the kernel's tests run on.
#[cfg(test)]
#[path = "toys.test.rs"]
pub(crate) mod toys;

// Aggregates, a process manager, policies and a projector written in
// angzarr-embedded-macros' markup (`markup` is its runtime), on angzarr-embedded as a dependent
// sees it (`::angzarr-embedded`), beside a hand-written domain.
#[cfg(test)]
#[path = "decorated.test.rs"]
mod decorated;

// An aggregate and a policy written by hand, with no macro, through
// markup's public functions alone, on angzarr-embedded as a dependent sees it.
#[cfg(test)]
#[path = "hand_written.test.rs"]
mod hand_written;

// The kernel over the shared SQLite adapter.
#[cfg(test)]
#[path = "sqlite.test.rs"]
mod sqlite;

// The deprecated `next` alias, as a dependent's generated code names it.
#[cfg(test)]
#[path = "next.test.rs"]
mod next_alias;

// A store recording each write, and failing what a test says.
#[cfg(test)]
#[path = "recording.test.rs"]
pub(crate) mod recording;

/// angzarr-diagnostic, the crate every error here implements
/// (`Diagnostic`, `Arg`, `Report`, `display`, and its `catalog` parity
/// check), re-exported so a dependent names the one this crate is built on.
pub use angzarr_diagnostic;
/// angzarr-ports, angzarr's shared storage contract, which the kernel runs
/// on, re-exported. Its en-US catalog, the messages for the store errors
/// this crate forwards under their own codes ([`KernelError::Store`]), is
/// `angzarr_ports::EN_US`.
pub use angzarr_ports;
/// angzarr's shared client router, which the kernel dispatches through.
pub use angzarr_router;
pub use check::{check, Body, Checked, Identified, Malformed, Sampled};
pub use conflict::{Key, UnitPath, Units};
pub use dead_letter::{dead_letter_line, DeadLetterLog, MemoryDeadLetters, DEAD_LETTER_PREFIX};
pub use domain::{Domain, State};
pub use error::{
    class, ChainProblem, Class, Failure, KernelError, PayloadError, Recovery, ReplayError, Step,
    StepError, UpcastChainError, UpcastError,
};
pub use fixture::{assert_replay_fixtures, check_replay_fixtures, ReplayFixture};
pub use handle::{
    Handled, Kernel, OnPolicyBug, OnRetryable, Project, ProjectionRoute, ProjectorFailure,
    Recorded, Wait, DEFAULT_DEPTH_LIMIT, DEFAULT_RACE_RUNS,
};
pub use handler::{Committed, Handler};
pub use payload::{decode_as, pack, type_name, Decoder, Payload};
pub use policy::{
    policy, projection, target_cover, translated, CommandTarget, EventSource, Order, Policy,
    PolicyHandler, Projection, Projector,
};
pub use registry::{Aggregate, Description, Misconfigured, Registry};
pub use route::{rejection, Rejected};
pub use store::{
    Append, ChangeToken, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey,
    PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice,
    SnapshotStore, Store, StoreError, StreamRef, Watch, Write, Written,
};
pub use stream::{Envelope, Executed, Stream};
pub use target::{Coordinated, CoordinatorPort, Target};
pub use upcast::{UpcastFn, UpcastPort, Upcaster};
pub use wire::WireError;

/// angzarr's id helpers: `Display`/`FromStr` for [`v1::Uuid`] (text only at
/// display and input), and the UUIDv5 derivations ([`ids::compute_root`],
/// [`ids::policy_command_id`], [`ids::fact_command_id`],
/// [`ids::fact_correlation`]).
pub use angzarr_proto::ids;
/// angzarr's wire contract, as the kernel uses it: the `io.angzarr.v1`
/// messages.
pub use angzarr_proto::v1;
/// The well-known types the kernel's pages carry: payloads are
/// [`Any`], instants [`Timestamp`].
pub use prost_types::{Any, Timestamp};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/angzarr-embedded/locales/en-US/angzarr-embedded.ftl`.
pub const EN_US: (&str, &str) = (
    "angzarr-embedded",
    include_str!("../locales/en-US/angzarr-embedded.ftl"),
);

/// The kernel's old namespace: everything here was `angzarr_embedded::next`
/// until it moved to the crate root. Kept only so code generated by an
/// angzarr-cli that still writes `::angzarr_embedded::next::…` compiles
/// until it is re-pinned (architecture-fixes B4), then removed.
#[deprecated(
    note = "the kernel is at the crate root: write `angzarr_embedded::…`, not \
            `angzarr_embedded::next::…`; this alias goes once the cli and its \
            dependents are re-pinned (architecture-fixes B4)"
)]
pub mod next {
    pub use crate::*;
}

/// The later of two instants: what `recorded_at` is raised to, so it never
/// goes backwards within a stream. prost's `Timestamp` has no ordering of
/// its own; this compares `(seconds, nanos)`.
pub fn not_before(floor: Option<&Timestamp>, now: Timestamp) -> Timestamp {
    let instant = |t: &Timestamp| (t.seconds, t.nanos);
    let (seconds, nanos) = floor.map_or(instant(&now), |f| instant(f).max(instant(&now)));
    Timestamp { seconds, nanos }
}

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in
/// `crates/angzarr-embedded/locales/en-US/angzarr-embedded.ftl`.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    use angzarr_diagnostic::Report;
    use angzarr_ports::Cause;
    use v1::ConflictResolution;

    let stream = || v1::Cover {
        domain: "account".to_string(),
        root: Some(v1::Uuid::from_bytes([7; 16])),
        edition: Some("what-if".to_string()),
    };
    let missing = WireError::Missing {
        message: "toy.v1.Deposited",
        field: "at",
    };
    let report = || Report::of(&missing);
    let domain = || "account".to_string();
    let malformed = [
        Malformed::NoCover,
        Malformed::UnknownDomain { domain: domain() },
        Malformed::NoRoot,
        Malformed::BadRoot { len: 15 },
        Malformed::EmptyEdition,
        Malformed::NoPage,
        Malformed::NoBody,
        Malformed::NoPayload,
        Malformed::BadCommandId { len: 1 },
        Malformed::BadCorrelationId { len: 17 },
        Malformed::CausationFromOutside,
        Malformed::IdOnPolicyCommand {
            field: "command_id",
        },
        Malformed::FactFromPolicy,
        Malformed::HandlingOnPolicyCommand,
        Malformed::BadTrigger(ids::UuidError::MissingTrigger),
        Malformed::FactWithoutOrigin,
        Malformed::EmptyExternalId,
        Malformed::CommandWithFactOrigin,
        Malformed::FactCommandIdNotDerived,
        Malformed::HandlingOnFact,
        Malformed::UnknownConflictResolution { value: 9 },
        Malformed::RefusalMisaddressed,
        Malformed::UnsupportedConflictResolution {
            domain: domain(),
            requested: ConflictResolution::Commutative,
        },
    ];
    let kernel = [
        KernelError::Stale {
            stream: stream(),
            expected: 2,
            actual: 4,
        },
        KernelError::ExpectedSequenceAhead {
            stream: stream(),
            expected: 7,
            head: 3,
        },
        KernelError::RetryCapExhausted {
            stream: stream(),
            attempts: 3,
            cap: 3,
        },
        KernelError::Unresolved {
            stream: stream(),
            expected: 2,
            actual: 4,
        },
        KernelError::RulesVersionMismatch {
            stream: stream(),
            registered: "1.4.2+build.7".to_string(),
            reported: "1.5.0+build.9".to_string(),
        },
        KernelError::Overlap {
            stream: stream(),
            expected: 2,
            actual: 4,
            window_units: "items[a],items[b]".to_string(),
            command_units: "items[b].count".to_string(),
        },
        KernelError::UnknownType {
            stream: stream(),
            type_url: "/toy.v9.Bonus".to_string(),
        },
        KernelError::InvalidPayload {
            stream: stream(),
            type_url: "/toy.v1.Deposit".to_string(),
            reason: report(),
        },
        KernelError::Upcast {
            stream: stream(),
            error: UpcastError::Rule {
                source: "toy.v0.Credited".to_string(),
                error: report(),
            },
        },
        KernelError::WrongDomain {
            stream: stream(),
            expected: "ledger",
        },
        KernelError::ChainTooDeep { limit: 8 },
        KernelError::TooManySteps {
            stream: stream(),
            limit: 16,
        },
        KernelError::BeforeSnapshot {
            stream: stream(),
            first: 10,
        },
        KernelError::UnregisteredDomain { domain: domain() },
        KernelError::Unreadable {
            stream: stream(),
            sequence: 0,
            reason: Report::of(&PayloadError::UnknownType {
                type_url: "/toy.v9.Bonus".to_string(),
            }),
        },
        KernelError::HandlerUnavailable {
            stream: stream(),
            cause: "not-running".to_string(),
        },
    ];
    let replay = [
        ReplayError::OutOfSequence { index: 1, found: 2 },
        ReplayError::RecordedBackwards { sequence: 4 },
        ReplayError::IncompleteMeta {
            sequence: 4,
            field: "correlation_id",
        },
        ReplayError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account",
        },
    ];
    // The store's errors are angzarr-ports': forwarded under their own
    // codes, every one a step's reads and write, or an open migrating the
    // store, can meet (an edition's creation, deletion and removal are the
    // application's).
    let boxed = || Box::new(stream());
    let store = [
        StoreError::Conflict {
            stream: boxed(),
            expected: 1,
            actual: 2,
        },
        StoreError::Discontinuous {
            stream: boxed(),
            expected: 2,
            found: 3,
        },
        StoreError::MalformedStream { stream: boxed() },
        StoreError::Unavailable(Cause::new(
            "angzarr-store-sqlite-unavailable",
            [
                ("errno", angzarr_ports::Arg::Uint(5)),
                (
                    "ext_err_txt",
                    angzarr_ports::Arg::Str("database is locked".to_string()),
                ),
            ],
        )),
        StoreError::Unreadable {
            stream: boxed(),
            sequence: 0,
            reason: Cause::new(
                "angzarr-store-sqlite-undecodable",
                [(
                    "ext_err_txt",
                    angzarr_ports::Arg::Str("buffer underflow".to_string()),
                )],
            ),
        },
        StoreError::EditionNotForked { stream: boxed() },
        StoreError::EditionNotFound {
            name: "what-if".to_string(),
        },
        StoreError::NewerThanBuild {
            applied: 6,
            build: 5,
        },
        StoreError::MigrationFailed { version: 3 },
        StoreError::MigrationChanged { version: 2 },
        StoreError::MigrationMissing { version: 2 },
        StoreError::MigrationUnknown { version: 7 },
        StoreError::MigrationIrreversible { version: 4 },
        StoreError::BackupExists {
            target: "/p/state.db.pre-4-20261031T120000.000Z".to_string(),
        },
    ]
    .map(KernelError::Store);
    let payload = [
        PayloadError::Invalid {
            type_url: "/toy.v1.Deposited".to_string(),
            reason: Report::of(&missing),
        },
        PayloadError::UnknownType {
            type_url: "/toy.v9.Bonus".to_string(),
        },
        PayloadError::Decode {
            type_url: "/toy.v1.Deposited".to_string(),
            error: <Timestamp as prost::Message>::decode(&[0x08][..])
                .expect_err("a tag without its value is not a message"),
        },
    ];
    let chain = [
        ChainProblem::Duplicate {
            source: "toy.v0.Credited".to_string(),
        },
        ChainProblem::OutOfOrder {
            source: "toy.v1.Credited".to_string(),
            produced_by: "toy.v0.Credited".to_string(),
        },
        ChainProblem::DeadEnd {
            source: "toy.v0.Credited".to_string(),
            ends_at: "toy.v9.Bonus".to_string(),
        },
    ]
    .map(|problem| UpcastChainError {
        domain: domain(),
        problem,
    });
    let misconfigured = [
        Misconfigured::AlreadyRegistered { domain: domain() },
        Misconfigured::UpcasterForAnotherDomain {
            domain: domain(),
            upcaster: "ledger".to_string(),
        },
        Misconfigured::EmptyRulesVersion { domain: domain() },
        Misconfigured::BadStateDescriptor { domain: domain() },
        Misconfigured::UnsupportedResolution {
            domain: domain(),
            command_type: None,
            resolution: ConflictResolution::Commutative,
        },
        Misconfigured::UnsupportedResolution {
            domain: domain(),
            command_type: Some("toy.cmd.v1.Note".to_string()),
            resolution: ConflictResolution::Commutative,
        },
        Misconfigured::RemoteSource {
            domain: domain(),
            component: "bookkeeping".to_string(),
        },
    ];

    let mut samples: Vec<Box<dyn angzarr_diagnostic::Diagnostic>> = Vec::new();
    samples.extend(malformed.map(|e| Box::new(e) as Box<_>));
    samples.extend(kernel.map(|e| Box::new(e) as Box<_>));
    samples.extend(replay.map(|e| Box::new(e) as Box<_>));
    samples.extend(store.map(|e| Box::new(e) as Box<_>));
    samples.extend(payload.map(|e| Box::new(e) as Box<_>));
    samples.extend(chain.map(|e| Box::new(e) as Box<_>));
    samples.extend(misconfigured.map(|e| Box::new(e) as Box<_>));
    samples.push(Box::new(UpcastError::Unavailable {
        cause: Report::of(&KernelError::HandlerUnavailable {
            stream: stream(),
            cause: "not-running".to_string(),
        }),
    }));
    samples.push(Box::new(UpcastError::Rule {
        source: "toy.v0.Credited".to_string(),
        error: report(),
    }));
    samples.push(Box::new(Failure::<WireError>::Downstream {
        path: Vec::new(),
        error: StepError::Kernel(KernelError::ChainTooDeep { limit: 8 }),
    }));
    samples.push(Box::new(Failure::<WireError>::Projection {
        committed: Vec::new(),
        failures: vec![("balances".to_string(), report())],
    }));
    for reason in route::forwarded() {
        samples.push(Box::new(KernelError::Router(route::internal(reason, []))));
    }
    samples.push(Box::new(missing));
    samples.push(Box::new(WireError::UnknownEnum {
        message: "toy.v1.Closed",
        field: "reason",
        value: 0,
    }));
    samples
}

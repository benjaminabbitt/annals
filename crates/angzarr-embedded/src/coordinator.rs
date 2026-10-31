//! Steps 3 to 5 of the pipeline: the in-process coordinator, over
//! angzarr-router.
//!
//! angzarr's aggregate coordinator is a service that loads a stream, checks
//! or stamps the command's expected sequence and hands a
//! `ContextualCommand` to business code. angzarr-embedded has no service and
//! sends nothing: the chain does the same steps in process, through the
//! store, domain-blind (it sees `Cover`s, pages and the [`Registry`], never
//! a domain's types), and hands the `ContextualCommand` to the domain's
//! router (its `CommandHandler`), which owns the business rules:
//!
//! 3. [`deduplicate`]: **load the stream first** (resolve the conflict
//!    resolution: the book's, the command type's, the domain's, else
//!    COMMUTATIVE where the state supports it and STRICT otherwise,
//!    [`Registry::resolve`]; then the newest usable snapshot (of the
//!    registered state type, its rules version compatible with the
//!    registered one, [`Registry::snapshot_usable`], and below the head) and the events
//!    after it), **then** look the command id up (the store's
//!    `find_command`), and [`admit`] the input, the kernel's own
//!    (decision 14.42): what an input meets before its handler runs (a
//!    refusal fact's address, deduplication by the events found, a
//!    process manager's start rule). Events found are answered [`Deduplicated::Duplicate`],
//!    and nothing below runs. The load comes first so that a concurrent
//!    commit of the same command (a retry racing its original) can never
//!    slip between the two reads unseen: if it lands after the load, either
//!    the lookup sees its events, or this command's append, expecting the
//!    head it loaded, fails as a conflict and the retry then finds the
//!    duplicate. Before the sequence check, so a retry of an applied STRICT
//!    command whose expected sequence is now stale is a duplicate, never
//!    stale.
//! 4. [`load`]: stamp an absent expected sequence with the head, or check a
//!    present one: under STRICT it must be the head ([`KernelError::Stale`]);
//!    under AGGREGATE_HANDLES and COMMUTATIVE the events from it to the head
//!    are the window, delivered as events (the snapshot used is at or below
//!    expected−1, so none of the window is hidden in snapshot state), with a
//!    nonce. An expected sequence past the head is malformed under every
//!    resolution ([`KernelError::ExpectedSequenceAhead`], decision 14.47).
//! 5. [`dispatch`]: the `ContextualCommand` (the stream's snapshot and
//!    events, upcast; the book with its ids and its handling resolved, its
//!    payload upcast through the domain's port before step 3 decided
//!    anything by its type, decision 14.39; the nonce) through the router, whose handler decides or records; the
//!    kernel stamps every event it answers with the input's
//!    [`Identified::meta`] and the rules version the answer reports, which
//!    must be the registered one. The router catches a panicking handler, a bug.

use angzarr_proto::v1::angzarr_dead_letter::Payload;
use angzarr_proto::v1::{
    command_page, CommandBook, ConflictResolution, Cover, EventBook, EventPage, Handling, Snapshot,
};
use angzarr_router::error::{extras, BusinessStatus, CodedError, RouterReason};

use super::reasons::{args, CoordinatorReason};
use angzarr_router::pb::ContextualCommand;
use prost_types::{Any, Timestamp};

use super::check::{Identified, Malformed, Sampled};
use super::dead_letter::{self, Cause, Component, DeadLetterLog};
use super::error::{Failure, KernelError, UpcastError};
use super::markup::STEP_LIMIT;
use super::payload::type_name;
use super::registry::Registry;
use super::route::{self, Rejected};
use super::store::Port;
use super::upcast::{upcast_input, upcast_pages};

/// What step 3 found.
pub(crate) enum Deduplicated {
    /// The stream holds this command's events already: they are the
    /// answer (angzarr's DUPLICATE), in sequence order, in an `EventBook`
    /// for the addressed stream.
    Duplicate { events: EventBook },
    /// A process manager's fact for a stream with no instance, of a type
    /// that does not start one: skipped, answered applied with no events.
    Skipped,
    /// Nothing found: the command goes on to [`load`]. A command whose
    /// earlier decision emitted nothing lands here again (an accepted cost:
    /// it is decided again, against the stream as it is now).
    Novel(Box<Novel>),
}

/// An [`Identified`] command that deduplication did not find, with the
/// stream it loaded first: the only thing [`load`] takes.
pub(crate) struct Novel {
    command: Identified,
    loaded: Loaded,
}

/// What step 3 read, before its lookup: the resolution, the snapshot, the
/// events after it, the head and the last instant.
pub(crate) struct Loaded {
    /// The input's payload upcast (decision 14.39), or why it could not be.
    current: Result<Any, UpcastError>,
    resolution: ConflictResolution,
    snapshot: Option<Snapshot>,
    pages: Vec<EventPage>,
    head: u64,
    last_recorded_at: Option<Timestamp>,
}

/// A command with its stream's history and its handling resolved: angzarr's
/// `ContextualCommand`, in process. Domain-blind: the history is pages.
pub(crate) struct Contextual {
    pub(crate) command: Identified,
    /// The command's payload upcast (decision 14.39): what the router is
    /// handed in its place.
    pub(crate) current: Any,
    /// Never UNSPECIFIED.
    pub(crate) resolution: ConflictResolution,
    /// The book's, or the head, stamped.
    pub(crate) expected: u64,
    /// The newest usable snapshot, at or below expected−1 under
    /// AGGREGATE_HANDLES and COMMUTATIVE.
    pub(crate) snapshot: Option<Snapshot>,
    /// The events after the snapshot (or from 0) to the head.
    pub(crate) pages: Vec<EventPage>,
    /// The stream's next sequence.
    pub(crate) head: u64,
    /// The last event's `recorded_at`: the floor for the next.
    pub(crate) last_recorded_at: Option<Timestamp>,
    /// Under AGGREGATE_HANDLES and COMMUTATIVE; `None` under STRICT.
    pub(crate) nonce: Option<Vec<u8>>,
}

impl Contextual {
    /// The stream's next sequence.
    pub fn head(&self) -> u64 {
        self.head
    }
}

/// The refusal for an incoming book whose domain the registry does not
/// hold: the caller's.
pub(crate) fn unknown_domain(domain: &str) -> KernelError {
    KernelError::Malformed(Malformed::UnknownDomain {
        domain: domain.to_string(),
    })
}

/// The router's refusal `error` of an input to `command`'s stream, in the
/// kernel's terms: a business refusal ([`route::is_rejection`]: a
/// FAILED_PRECONDITION, or a handler's own code under another business
/// status) is [`Failure::Rejected`]; a decision past its step limit
/// [`KernelError::TooManySteps`]; anything else, a handler's own code under
/// any other status and a policy's failure in its source's `#[on_refused]`
/// handler included (the chain makes it
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed)),
/// [`KernelError::Router`].
pub(crate) fn refused(command: &Identified, error: CodedError) -> Failure<Rejected> {
    if error.code == route::TOO_MANY_STEPS {
        return Failure::Kernel(KernelError::TooManySteps {
            stream: command.cover().clone(),
            limit: STEP_LIMIT,
        });
    }
    if error.code == route::HANDLER_UNAVAILABLE {
        return Failure::Kernel(KernelError::HandlerUnavailable {
            stream: command.cover().clone(),
            cause: error.extras.get("cause").cloned().unwrap_or_default(),
        });
    }
    if route::is_rejection(&error) {
        return Failure::Rejected(Rejected::of(&error));
    }
    Failure::Kernel(KernelError::Router(error))
}

/// Step 3 over `port`: load the stream, look the command up, and
/// [`admit`] it.
pub(crate) async fn deduplicate<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: Identified,
) -> Result<Deduplicated, Failure<Rejected>> {
    let loaded = read(port, registry, &command)
        .await
        .map_err(Failure::Kernel)?;
    let recorded = port
        .find_command(command.cover(), command.command_id())
        .await
        .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
    match admit(
        registry,
        &command,
        &loaded.current,
        recorded,
        loaded.head > 0,
    )? {
        Admitted::Duplicate(pages) => Ok(Deduplicated::Duplicate {
            events: EventBook {
                cover: Some(command.cover().clone()),
                snapshot: None,
                next_sequence: pages.last().map_or(0, |p| p.sequence + 1),
                pages,
            },
        }),
        Admitted::Skip => Ok(Deduplicated::Skipped),
        Admitted::Dispatch => Ok(Deduplicated::Novel(Box::new(Novel { command, loaded }))),
    }
}

/// What admission decided.
#[derive(Debug, PartialEq)]
pub(crate) enum Admitted {
    /// The stream holds the input's events already: these.
    Duplicate(Vec<EventPage>),
    /// A process manager's fact for a stream with no instance, of a type
    /// that does not start one: skipped.
    Skip,
    /// On to its handler.
    Dispatch,
}

/// Admission, the kernel's own (decision 14.42; the router admits
/// nothing): what an input meets before its handler runs, from what
/// step 3 read (`recorded`, the events found under its id; whether its
/// stream `exists`), in order:
/// 1. a refusal fact (a fact with a causation) is admitted only by the
///    exact stream its causation names (`COORDINATOR_REASON_REFUSAL_FACT_WRONG_STREAM`, a bug);
/// 2. events found under its id are [`Admitted::Duplicate`];
/// 3. a process manager's start rule: with no instance, an input whose type
///    after upcasting (`current`) is not one of its starting types is
///    refused `COORDINATOR_REASON_PM_NO_INSTANCE` (a business refusal) if a command, and
///    [`Admitted::Skip`]ped if a fact. Its upcast's failure is
///    [`KernelError::Upcast`].
pub(crate) fn admit(
    registry: &Registry,
    command: &Identified,
    current: &Result<Any, UpcastError>,
    recorded: Vec<EventPage>,
    exists: bool,
) -> Result<Admitted, Failure<Rejected>> {
    let is_fact = command.body().is_fact();
    if let (true, Some(cause)) = (is_fact, command.causation_id()) {
        if cause.stream.as_ref() != Some(command.cover()) {
            return Err(Failure::Kernel(KernelError::Router(route::internal(
                CoordinatorReason::RefusalFactWrongStream.as_str_name(),
                [(args::DOMAIN.to_string(), command.domain().to_string())],
            ))));
        }
    }
    if !recorded.is_empty() {
        return Ok(Admitted::Duplicate(recorded));
    }
    let Some(starting) = registry.starting_types(command.domain()) else {
        return Ok(Admitted::Dispatch);
    };
    if exists {
        return Ok(Admitted::Dispatch);
    }
    let current = current.as_ref().map_err(|error| {
        Failure::Kernel(KernelError::Upcast {
            stream: command.cover().clone(),
            error: error.clone(),
        })
    })?;
    if starting.contains(type_name(&current.type_url)) {
        return Ok(Admitted::Dispatch);
    }
    if is_fact {
        return Ok(Admitted::Skip);
    }
    Err(Failure::Rejected(Rejected::of(&CodedError::rejection(
        BusinessStatus::FailedPrecondition,
        CoordinatorReason::PmNoInstance.as_str_name(),
        [(
            extras::TYPE_URL.to_string(),
            command.payload().type_url.clone(),
        )],
    ))))
}

/// `command`'s payload upcast through its domain's port (decision 14.39),
/// before the kernel decides anything by its type: as sent when the domain
/// has no port.
pub(crate) async fn upcast(registry: &Registry, command: &Identified) -> Result<Any, UpcastError> {
    match registry.upcast_port(command.domain()) {
        Some(port) => upcast_input(port, command.domain(), command.payload()).await,
        None => Ok(command.payload().clone()),
    }
}

/// `book` carrying `payload` in its body's place, the arm kept.
pub(crate) fn with_payload(mut book: CommandBook, payload: &Any) -> CommandBook {
    if let Some(body) = book.page.as_mut().and_then(|page| page.body.as_mut()) {
        match body {
            command_page::Body::Command(c) => c.payload = Some(payload.clone()),
            command_page::Body::Fact(f) => f.payload = Some(payload.clone()),
        }
    }
    book
}

/// The command's resolution ([`Registry::resolve`]): never UNSPECIFIED.
/// The configuration per command type is looked up by the command's
/// `current` type, after upcasting, so an old type has the setting of the
/// type it upcasts to. A payload whose upcast failed is looked up as sent;
/// the failure is reported at [`load`].
pub(crate) fn resolution(
    registry: &Registry,
    command: &Identified,
    current: &Result<Any, UpcastError>,
) -> Result<ConflictResolution, KernelError> {
    let domain = command.domain();
    let current = current.as_ref().unwrap_or(command.payload());
    registry
        .resolve(domain, &current.type_url, command.conflict_resolution())
        .ok_or_else(|| unknown_domain(domain))
}

/// The expected sequence, stamped with `head` or checked against it, and
/// the nonce (from `nonces`) under AGGREGATE_HANDLES and COMMUTATIVE. One
/// ahead of the head is malformed under every resolution
/// ([`KernelError::ExpectedSequenceAhead`], decision 14.47); one behind it
/// is stale under STRICT ([`KernelError::Stale`]).
pub(crate) fn expect(
    command: &Identified,
    resolution: ConflictResolution,
    head: u64,
    nonces: &mut u64,
) -> Result<(u64, Option<Vec<u8>>), KernelError> {
    let expected = command.expected_sequence().unwrap_or(head);
    if expected > head {
        return Err(KernelError::ExpectedSequenceAhead {
            stream: command.cover().clone(),
            expected,
            head,
        });
    }
    if resolution == ConflictResolution::Strict && expected != head {
        return Err(KernelError::Stale {
            stream: command.cover().clone(),
            expected,
            actual: head,
        });
    }
    let nonce = (resolution != ConflictResolution::Strict).then(|| {
        *nonces += 1;
        nonces.to_be_bytes().to_vec()
    });
    Ok((expected, nonce))
}

/// The read half of step 3: the resolution, the newest usable snapshot and
/// the events after it. A snapshot at or past the head is not usable: the
/// stream is read from 0.
pub(crate) async fn read<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: &Identified,
) -> Result<Loaded, KernelError> {
    let current = upcast(registry, command).await;
    let resolution = resolution(registry, command, &current)?;
    let cover = command.cover();
    let windowed = resolution != ConflictResolution::Strict;
    let snapshot = port
        .read_snapshot(cover)
        .await
        .map_err(KernelError::Store)?
        .filter(|s| {
            registry.snapshot_usable(command.domain(), s)
                && !(windowed && command.expected_sequence().is_some_and(|e| s.sequence >= e))
        });
    let from = snapshot.as_ref().map_or(0, |s| s.sequence + 1);
    let mut slice = port
        .read_from(cover, from)
        .await
        .map_err(KernelError::Store)?;
    let snapshot = match (snapshot, &slice) {
        // At or past the head: it claims events the stream does not hold
        // (a purge left it, or it was written ahead of them), so it is
        // unusable, like one under other rules, and the stream is read
        // from its events.
        (Some(s), read) if s.sequence >= read.next_sequence => {
            slice = port.read_from(cover, 0).await.map_err(KernelError::Store)?;
            None
        }
        (snapshot, _) => snapshot,
    };
    Ok(Loaded {
        current,
        resolution,
        snapshot,
        pages: slice.pages,
        head: slice.next_sequence,
        last_recorded_at: slice.last_recorded_at,
    })
}

/// Step 4: the expected sequence stamped or checked against the head step 3
/// loaded.
pub(crate) fn load(novel: Novel, nonces: &mut u64) -> Result<Contextual, KernelError> {
    let Novel { command, loaded } = novel;
    let (expected, nonce) = expect(&command, loaded.resolution, loaded.head, nonces)?;
    let current = loaded.current.map_err(|error| KernelError::Upcast {
        stream: command.cover().clone(),
        error,
    })?;
    Ok(Contextual {
        command,
        current,
        resolution: loaded.resolution,
        expected,
        snapshot: loaded.snapshot,
        pages: loaded.pages,
        head: loaded.head,
        last_recorded_at: loaded.last_recorded_at,
        nonce,
    })
}

/// Step 5: `command` through its domain's router, against its stream's
/// history: the events it answers, stamped at the instant the input was
/// accepted, `recorded_at = max(sampled now, the stream's last)` (the
/// `ContextualCommand.recorded_at` its handler read), with the rules
/// version the answer reports, in full, from the head on. An answer
/// reporting another version than the registered one is a handler bug
/// (command_handler.proto, C-0558): [`KernelError::RulesVersionMismatch`],
/// nothing kept, dead-lettered. The router catches a handler's panic,
/// a bug (ROUTER_REASON_UNHANDLED_HANDLER_ERROR, cause ROUTER_REASON_HANDLER_PANICKED). A bug
/// ([`route::is_handler_fault`]) fails it as [`KernelError::Router`], and
/// `dead_letters` gets a line of its code and args.
pub(crate) async fn decide(
    command: &Contextual,
    sampled: &Sampled,
    registry: &Registry,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Decided, Failure<Rejected>> {
    let identified = &command.command;
    let domain = identified.domain();
    let kernel = Failure::Kernel;
    let handler = registry
        .handler(domain)
        .ok_or_else(|| kernel(unknown_domain(domain)))?;
    let contextual = contextual_command(command, registry, sampled.now)
        .await
        .map_err(kernel)?;
    let recorded_at = contextual.recorded_at.unwrap_or_default();
    let response = handler
        .dispatch(&contextual)
        .await
        .and_then(|response| judged(&contextual, response));
    let response = response.map_err(|error| {
        let error = match RouterReason::from_str_name(&error.code) {
            Some(RouterReason::UnknownCommandType | RouterReason::NoFactHandler) => {
                as_sent(error, identified.payload(), &command.current)
            }
            _ => error,
        };
        let (stream, expected, actual) =
            (identified.cover().clone(), command.expected, command.head);
        match CoordinatorReason::from_str_name(&error.code) {
            Some(CoordinatorReason::ConflictOverlap) => {
                let units = |key: &str| error.extras.get(key).cloned().unwrap_or_default();
                Failure::Kernel(KernelError::Overlap {
                    stream,
                    expected,
                    actual,
                    window_units: units(args::WINDOW_UNITS),
                    command_units: units(args::COMMAND_UNITS),
                })
            }
            Some(CoordinatorReason::ConflictUnresolved) => {
                let unresolved = KernelError::Unresolved {
                    stream,
                    expected,
                    actual,
                };
                dead_letter::write(
                    dead_letters,
                    Payload::RejectedCommand(identified.to_book()),
                    Component::domain(registry, domain),
                    Cause::Kernel(&unresolved),
                );
                Failure::Kernel(unresolved)
            }
            _ => {
                let failure = refused(identified, error);
                if let Failure::Kernel(error @ KernelError::Router(coded)) = &failure {
                    if route::is_handler_fault(coded) {
                        dead_letter::write(
                            dead_letters,
                            Payload::RejectedCommand(identified.to_book()),
                            Component::domain(registry, domain),
                            Cause::Kernel(error),
                        );
                    }
                }
                failure
            }
        }
    })?;
    // Read after the answer: a child described again is registered by its
    // new descriptor before it gets work, so its answer meets its version.
    let registered = registry.rules_version(domain).unwrap_or_default();
    if response.rules_version != registered {
        let mismatch = KernelError::RulesVersionMismatch {
            stream: identified.cover().clone(),
            registered,
            reported: response.rules_version,
        };
        dead_letter::write(
            dead_letters,
            Payload::RejectedCommand(identified.to_book()),
            Component::domain(registry, domain),
            Cause::Kernel(&mismatch),
        );
        return Err(Failure::Kernel(mismatch));
    }
    let rules_version = response.rules_version;
    let meta = identified.meta(recorded_at, &rules_version);
    let events = response.events.unwrap_or_default();
    let pages: Vec<EventPage> = (command.head..)
        .zip(
            events
                .pages
                .into_iter()
                .filter(|page| !route::is_nothing(page)),
        )
        .map(|(sequence, page)| EventPage {
            sequence,
            meta: Some(meta.clone()),
            ..page
        })
        .collect();
    let snapshot = match (pages.last(), events.snapshot.and_then(|s| s.state)) {
        (Some(last), Some(state)) => Some(Snapshot {
            sequence: last.sequence,
            state: Some(state),
            rules_version,
        }),
        _ => None,
    };
    Ok(Decided { pages, snapshot })
}

/// `response` to `delivered`, judged by its conflict report (decision
/// 14.42; [`conflict::judge`](super::conflict::judge)): the router's answer
/// when its events may be kept; otherwise the refusal the kernel answers
/// with, as the router would have, ABORTED: `COORDINATOR_REASON_CONFLICT_UNRESOLVED`, or
/// `COORDINATOR_REASON_CONFLICT_OVERLAP` naming both unit sets.
fn judged(
    delivered: &ContextualCommand,
    response: angzarr_router::pb::CommandHandlerResponse,
) -> Result<angzarr_router::pb::CommandHandlerResponse, CodedError> {
    use super::conflict::{judge, Judgement};
    let resolution = delivered
        .command
        .as_ref()
        .and_then(|book| book.handling.as_ref())
        .map_or(ConflictResolution::Strict, |h| h.conflict_resolution());
    match judge(
        resolution,
        &delivered.conflict_nonce,
        response.conflict.as_ref(),
    ) {
        Judgement::Kept => Ok(response),
        Judgement::Unresolved => Err(CodedError::immediate(
            CoordinatorReason::ConflictUnresolved.as_str_name(),
            [],
        )),
        Judgement::Overlap { window, command } => Err(CodedError::immediate(
            CoordinatorReason::ConflictOverlap.as_str_name(),
            [
                (args::WINDOW_UNITS.to_string(), window.join(",")),
                (args::COMMAND_UNITS.to_string(), command.join(",")),
            ],
        )),
    }
}

/// What step 5 decided: the events, stamped, and the snapshot to write
/// with them.
pub(crate) struct Decided {
    /// The events, from the head on.
    pub(crate) pages: Vec<EventPage>,
    /// The state the router returned with them, as the snapshot at the last
    /// event under the rules version they were stamped with (decision 14.40);
    /// `None` with no events, or for a state with no snapshot form.
    pub(crate) snapshot: Option<Snapshot>,
}

/// Step 5's events alone ([`decide`]): what a stream run in memory keeps.
pub(crate) async fn dispatch(
    command: &Contextual,
    sampled: &Sampled,
    registry: &Registry,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Vec<EventPage>, Failure<Rejected>> {
    decide(command, sampled, registry, dead_letters)
        .await
        .map(|decided| decided.pages)
}

/// Write `snapshot` of `stream` over the one it has (the default policy,
/// the newest only; decision 14.40). A snapshot is a cache: the events are
/// committed already, so a failed write fails nothing, and the next load
/// replays from the snapshot it finds.
pub(crate) async fn write_snapshot<P: Port>(
    port: &mut P,
    stream: &Cover,
    snapshot: Option<Snapshot>,
) {
    if let Some(snapshot) = snapshot {
        let _ = port.write_snapshot(stream, &snapshot).await;
    }
}

/// The router's unknown-type refusal `error`, as the caller is answered
/// (decision 14.39): `type_url` the type `sent`, `upcast_type_url` the type
/// the router was handed, `current`. The same with no rule applied.
fn as_sent(mut error: CodedError, sent: &Any, current: &Any) -> CodedError {
    error
        .extras
        .insert(extras::TYPE_URL.to_string(), sent.type_url.clone());
    error.extras.insert(
        extras::UPCAST_TYPE_URL.to_string(),
        current.type_url.clone(),
    );
    error
}

/// The `ContextualCommand` step 5 hands `command`'s domain: its stream's
/// history upcast, its handling resolved (COMMUTATIVE at the head is handed
/// over as STRICT, with no nonce), the delivery's nonce, and the instant
/// the input is accepted at, `max(now, the stream's last)`: what its events
/// are recorded at (decision 14.37, `ContextualCommand.recorded_at`).
pub(crate) async fn contextual_command(
    command: &Contextual,
    registry: &Registry,
    now: Timestamp,
) -> Result<ContextualCommand, KernelError> {
    let identified = &command.command;
    let pages = match registry.upcast_port(identified.domain()) {
        Some(port) => upcast_pages(port, identified.domain(), &command.pages).await,
        None => Ok(command.pages.clone()),
    }
    .map_err(|error| KernelError::Upcast {
        stream: identified.cover().clone(),
        error,
    })?;
    // At the head there is no window: nothing for COMMUTATIVE to compare,
    // so the command is handed over as STRICT, with no nonce.
    let (resolution, nonce) = match command.resolution {
        ConflictResolution::Commutative if command.expected == command.head => {
            (ConflictResolution::Strict, None)
        }
        resolution => (resolution, command.nonce.clone()),
    };
    let mut book = with_payload(identified.to_book(), &command.current);
    book.handling = Some(Handling {
        expected_sequence: Some(command.expected),
        conflict_resolution: resolution as i32,
    });
    Ok(ContextualCommand {
        events: Some(EventBook {
            cover: Some(identified.cover().clone()),
            snapshot: command.snapshot.clone(),
            pages,
            next_sequence: command.head,
        }),
        command: Some(book),
        conflict_nonce: nonce.unwrap_or_default().into(),
        recorded_at: Some(crate::not_before(command.last_recorded_at.as_ref(), now)),
    })
}

#[cfg(test)]
#[path = "steps.test.rs"]
pub(crate) mod steps;

#[cfg(test)]
#[path = "coordinator.test.rs"]
mod tests;

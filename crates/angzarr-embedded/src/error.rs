//! Errors, kept apart by whose they are: a domain's rejection is for the
//! person who gave the command; a [`Malformed`] book is the caller's; a
//! [`KernelError`] is a fault in delivery, storage or a log, for the
//! operator.
//!
//! Each is a [`Diagnostic`], its codes under `angzarr-embedded-*` and its
//! messages in `locales/en-US/angzarr-embedded.ftl`; a wrapper of another error
//! ([`Failure::Rejected`], [`Failure::Kernel`], [`StepError`],
//! [`KernelError::Malformed`], [`KernelError::Replay`],
//! [`KernelError::Store`], [`UpcastError::Payload`]) is that error's
//! diagnostic. The store's errors are angzarr-ports' `StoreError`, shared
//! with angzarr: [`KernelError::Store`] forwards its `angzarr-ports-*` code
//! and args unchanged, and angzarr-embedded's catalog holds their messages. Every error about a payload names its type URL; a stream is
//! named as `domain/root`, with `@edition` outside the main timeline.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, Report};
use angzarr_ports::StoreError;
use angzarr_proto::v1::{Cover, EventBook, EventRef, Uuid};

use super::check::Malformed;

/// Why a command, or a step its chain set off, failed.
#[derive(Clone, PartialEq, Debug)]
pub enum Failure<R> {
    /// The domain refused it, deliberately: under its own code, a business
    /// status (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
    /// ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED); under the
    /// router's, a FAILED_PRECONDITION. Nothing was staged, appended or
    /// applied.
    Rejected(R),
    /// A step further down the chain failed: a policy's command rejected by
    /// its target, a malformed policy book, a gap, a chain too deep, a store
    /// that failed a step's write. `path` runs from the first policy step to
    /// the one that failed. Every step before it stays committed, the
    /// originating command's included; the failed step wrote nothing but,
    /// where it was refused, its position past it.
    Downstream { path: Vec<Step>, error: StepError },
    /// The book was malformed, or the kernel or the store failed at the
    /// originating stream, its write included (a conflict). Nothing was
    /// committed.
    Kernel(KernelError),
    /// The chain COMMITTED, then marked-up projectors (run after the
    /// commit and awaited) failed: each `(projector, error)` in
    /// `failures`, in registration order; a failing projector does not
    /// stop the others. `committed` is what the chain appended, every
    /// stream's book: those events stay committed. Its recovery is
    /// [`Recovery::Retry`]: a retry is answered DUPLICATE.
    Projection {
        committed: Vec<EventBook>,
        failures: Vec<(String, Report)>,
    },
}

/// One step of a command chain: a policy's command delivered to `stream`,
/// caused by `cause`. Policies are pure, so the step's command is the
/// policy's output for that cause, recomputable from the log.
#[derive(Clone, PartialEq, Debug)]
pub struct Step {
    pub stream: Cover,
    pub cause: EventRef,
}

/// Why a step down the chain failed.
#[derive(Clone, PartialEq, Debug)]
pub enum StepError {
    /// The target's decide rejected the command: its rejection, as a
    /// report.
    Rejected(Report),
    /// A kernel fault at that step. A malformed policy book is a bug here,
    /// not the caller's: it is also written to the dead-letter log.
    Kernel(KernelError),
    /// The policy itself failed: its `on_event`, its `on_refused` (a
    /// translating callback) or its source's `#[on_refused]` handler
    /// answered an error, or a marked event did not decode. A defect, never a business refusal: policies react to
    /// events the application's own domains recorded, so an error is a
    /// bug. Its report (code and args) is the `cause` of the dead letter's
    /// reason, `ROUTER_REASON_UNHANDLED_HANDLER_ERROR`, and the
    /// policy's position moved past the trigger (or stayed at it, under
    /// [`OnPolicyBug::Stay`](super::OnPolicyBug::Stay)).
    PolicyFailed(Report),
}

/// A fault outside any domain's rules.
#[derive(Clone, PartialEq, Debug)]
pub enum KernelError {
    /// The book was refused at step 1, before anything else ran.
    Malformed(Malformed),
    /// Under STRICT, the stream is past the sequence expected: a book's
    /// `expected_sequence` behind the head when it is loaded, or a write
    /// lost to another write to the same stream (decision 14.45). angzarr's
    /// ABORTED, `COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE`: re-read, then
    /// re-issue.
    Stale {
        stream: Cover,
        expected: u64,
        actual: u64,
    },
    /// A book's `expected_sequence` ahead of the stream's head, under any
    /// resolution: it names a state the stream never held, so the command
    /// is malformed (decision 14.47), refused when it is loaded, before
    /// business code. angzarr's INVALID_ARGUMENT,
    /// `COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD`: fix the command.
    ExpectedSequenceAhead {
        stream: Cover,
        expected: u64,
        head: u64,
    },
    /// COMMUTATIVE's window and the command changed overlapping units
    /// (angzarr's ABORTED, `COORDINATOR_REASON_CONFLICT_OVERLAP`; decision 14.42): the
    /// command's events were discarded. `window_units` and `command_units`
    /// are the two sets as the metadata carries them, comma-separated unit
    /// paths. Re-read, then re-issue.
    Overlap {
        stream: Cover,
        expected: u64,
        actual: u64,
        window_units: String,
        command_units: String,
    },
    /// A decide under AGGREGATE_HANDLES returned `Ok` without acknowledging
    /// the window with this delivery's nonce: its events were discarded,
    /// and the command fails as stale would. A handler bug, logged loudly.
    Unresolved {
        stream: Cover,
        expected: u64,
        actual: u64,
    },
    /// The kernel gave up on a command or fact that lost a race at its
    /// write to another write to the same stream, every time, until a cap
    /// (decision 14.46): one stamped with the head it loaded (no expected
    /// sequence, or a fact), re-run on the new head, at the race re-run cap
    /// ([`OnRetryable::max_race_runs`](super::OnRetryable::max_race_runs));
    /// or an AGGREGATE_HANDLES or COMMUTATIVE one re-delivered its widened
    /// window, at the retry cap (`max_retries + 1` deliveries). `attempts`
    /// is how many times business code ran, `cap` the limit reached.
    /// Nothing is persisted; a policy's step is dead-lettered under this
    /// error, and a command its caller waits for is the caller's (owner
    /// ruling, 2026-10-31). angzarr's INTERNAL,
    /// `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED`: the retries are spent, so
    /// no caller retries it; a fact is answered PARKED instead
    /// ([`Handled::Parked`](super::Handled::Parked)), and dead-lettered.
    RetryCapExhausted {
        stream: Cover,
        attempts: u32,
        cap: u32,
    },
    /// The handler of `stream`'s domain answered reporting rules version
    /// `reported`, not `registered`, the version its domain is registered
    /// at (`CommandHandlerResponse.rules_version`, C-0558): a handler bug,
    /// refused with nothing persisted, and dead-lettered as every handler
    /// bug is. angzarr's INTERNAL, under the spec's key for a component
    /// whose answers report another version than its descriptor,
    /// `HOST_REASON_DESCRIPTOR_RULES_VERSION_MISMATCH` (component,
    /// described, reported).
    RulesVersionMismatch {
        stream: Cover,
        registered: String,
        reported: String,
    },
    /// A command's or fact's payload type that the domain does not decode,
    /// after the upcaster: refused naming its type URL (angzarr's
    /// INVALID_ARGUMENT). Found at step 5, after deduplication.
    UnknownType {
        stream: Cover,
        type_url: String,
    },
    /// A command's or fact's payload that decodes as a type the domain
    /// reads, but is not a valid value of it (a required field absent, an
    /// undefined enum value): the sender's to fix, as `UnknownType` is.
    InvalidPayload {
        stream: Cover,
        type_url: String,
        reason: Report,
    },
    /// A command's or fact's upcast rule failed.
    Upcast {
        stream: Cover,
        error: UpcastError,
    },
    /// A book or a log for `stream` was given to an `expected` domain's
    /// code.
    WrongDomain {
        stream: Cover,
        expected: &'static str,
    },
    /// A command chain went deeper than its limit: a policy cycle, or a
    /// cascade longer than allowed.
    ChainTooDeep {
        limit: usize,
    },
    /// A decision on `stream` ran more continuation steps than its limit
    /// ([`STEP_LIMIT`](super::markup::STEP_LIMIT)): a step that always
    /// continues. Nothing it emitted is kept.
    TooManySteps {
        stream: Cover,
        limit: usize,
    },
    /// A dispatch from before a resumed stream's snapshot: those events are
    /// not held.
    BeforeSnapshot {
        stream: Cover,
        first: u64,
    },
    /// A stored stream of `domain` was replayed, resumed or dispatched, or
    /// its typed step run, but the registry does not hold `domain`: the
    /// application did not register it. A misconfiguration, unlike an
    /// incoming book for an unregistered domain
    /// ([`Malformed::UnknownDomain`], the caller's).
    UnregisteredDomain {
        domain: String,
    },
    /// Event `sequence` of `stream` is stored but cannot be replayed as a
    /// current event: a payload type nothing upcasts or decodes (replay is
    /// strict), or bytes that do not decode as it. `reason` names what
    /// could not be read (the type URL). A bug or a missing upcaster.
    Unreadable {
        stream: Cover,
        sequence: u64,
        reason: Report,
    },
    Replay(ReplayError),
    /// The store's own error, angzarr-ports' (codes `angzarr-ports-*`). On
    /// the wire ([`KernelError::as_coded`]) a conflict is
    /// `COORDINATOR_REASON_STORE_CONFLICT` and a store that could not
    /// answer `COORDINATOR_REASON_STORE_UNAVAILABLE`, each UNAVAILABLE
    /// (decision 14.51).
    Store(StoreError),
    /// angzarr-router refused the input or a policy's answer: its
    /// `CodedError`, whose `code` (SCREAMING_SNAKE, angzarr's `ErrorInfo`
    /// reason) is this error's `reason`. A handler's own code under a
    /// business status is a refusal ([`Failure::Rejected`], never this),
    /// under any other status its bug; of the router's codes, INTERNAL is
    /// a policy's or a handler's bug, ABORTED a stale command, anything
    /// else the sender's to fix.
    Router(angzarr_router::error::CodedError),
    /// The domain's handler could not be reached (a remote handler whose
    /// process is not running, or whose transport failed: its
    /// `COORDINATOR_REASON_HANDLER_UNAVAILABLE`, UNAVAILABLE). Nothing ran
    /// and nothing was written:
    /// run it again, as [`Kernel::resume`](super::Kernel::resume) does for
    /// a policy's step.
    HandlerUnavailable {
        stream: Cover,
        cause: String,
    },
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// The page at `index` claims sequence `found`.
    OutOfSequence { index: usize, found: u64 },
    /// Event `sequence` was recorded before the event before it.
    RecordedBackwards { sequence: u64 },
    /// Event `sequence`'s `EventMeta` lacks `field` (`causation`,
    /// `correlation_id` or `recorded_at`), or holds it malformed: every
    /// recorded event has all three.
    IncompleteMeta { sequence: u64, field: &'static str },
    /// The log is `found`'s, given to an `expected` domain's code.
    WrongDomain {
        found: String,
        expected: &'static str,
    },
}

/// A payload that is not one of the types a domain decodes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PayloadError {
    /// No decoder for this type URL: not a current type of the domain's
    /// (strict: never skipped).
    UnknownType { type_url: String },
    /// The type is known, but its bytes do not decode as it.
    Decode {
        type_url: String,
        error: prost::DecodeError,
    },
    /// The bytes decode, but the message is not a valid domain value: a
    /// required field absent, an enum value the domain does not define
    /// ([`WireError`](super::WireError)), or a state's own invariant.
    Invalid { type_url: String, reason: Report },
}

/// An upcast rule that failed on one payload.
#[derive(Clone, PartialEq, Debug)]
pub enum UpcastError {
    /// The rule could not read its input.
    Payload(PayloadError),
    /// The rule from `source` failed for the domain's own reason.
    Rule { source: String, error: Report },
    /// The upcaster sidecar could not be reached, or answered under a
    /// retryable status (decision 14.39): what needs upcasting waits, and
    /// a caller is answered UNAVAILABLE, to retry. `cause` is why, as the
    /// sidecar's port reported it.
    Unavailable { cause: Report },
}

/// An upcaster whose rules cannot all run, found by
/// [`Registry::check`](super::Registry::check) before anything is read.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UpcastChainError {
    /// The domain, named in every error.
    pub domain: String,
    pub problem: ChainProblem,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChainProblem {
    /// Two typed rules from the same type: the second never runs.
    Duplicate { source: String },
    /// A typed rule from `source` registered before the rule that produces
    /// `source`: rules run in registration order, so a payload reaching
    /// `source` stops there.
    OutOfOrder { source: String, produced_by: String },
    /// A chain from `source` ends at `ends_at`, which the domain does not
    /// decode: every payload of that type would be refused.
    DeadEnd { source: String, ends_at: String },
}

/// What to do about a failure: retry, fix the request, or give up, and
/// why. The kernel retries a step's [`Recovery::Retry`] itself, with
/// backoff, up to a cap, then dead-letters it
/// ([`OnRetryable`](super::OnRetryable)); the others it never retries.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Recovery {
    /// Run the command again, with the same command id, in a fresh unit of
    /// work: a store conflict, a stale or unresolved command (re-read and
    /// re-issue against the new head), the store unavailable, a handler
    /// that could not be reached or answered under a retryable status
    /// (UNAVAILABLE, ABORTED, UNIMPLEMENTED and the like), a gap (catch the
    /// target up first). Safe, because a retry starts at the deduplication
    /// lookup: if the first attempt committed after all, the retry is
    /// answered DUPLICATE.
    Retry,
    /// The domain said no: a rejection, at the origin or downstream. The
    /// same command will be refused the same way; tell the person.
    Business,
    /// The caller's book was malformed, or names a payload type the domain
    /// does not decode (angzarr's INVALID_ARGUMENT). Fix the request.
    Invalid,
    /// A bug or a misconfiguration: a malformed policy book, a handler's
    /// bug or panic (a status of the bug class: INTERNAL, UNKNOWN,
    /// DATA_LOSS), the router's own error on malformed input, a
    /// discontinuous append, a key of the wrong domain, a chain too deep,
    /// unknown rules, a log that does not replay, an event the store cannot
    /// read back. Retrying will not help; an operator must.
    Bug,
}

impl<R> Failure<R> {
    pub fn recovery(&self) -> Recovery {
        match self {
            Failure::Rejected(_) => Recovery::Business,
            Failure::Downstream { error, .. } => error.recovery(),
            Failure::Kernel(error) => error.recovery(),
            Failure::Projection { .. } => Recovery::Retry,
        }
    }
}

impl StepError {
    pub fn recovery(&self) -> Recovery {
        match self {
            StepError::Rejected(_) => Recovery::Business,
            StepError::PolicyFailed(_) => Recovery::Bug,
            StepError::Kernel(KernelError::Malformed(_)) => Recovery::Bug,
            StepError::Kernel(error) => error.recovery(),
        }
    }
}

impl KernelError {
    /// This error as the coordinator answers it on the wire, in the
    /// router's shape: its reason key (a `CoordinatorReason`'s full name,
    /// `as_str_name`), its args as metadata, and its status; `None` for an
    /// error that is not one of the coordinator's answers:
    ///
    /// - an expected sequence ahead of the head, under any resolution, is
    ///   `COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD`, INVALID_ARGUMENT
    ///   (business, fatal; decision 14.47), args `expected` and `head`;
    ///
    /// and, each ABORTED (business, immediate return; decision 14.37):
    ///
    /// - a stale expected sequence, behind the head, under STRICT (the
    ///   early check, or a write lost to a racing command; decision 14.45)
    ///   is `COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE`, args `expected`
    ///   and `actual`;
    /// - a COMMUTATIVE overlap is `COORDINATOR_REASON_CONFLICT_OVERLAP`,
    ///   args `window_units` and `command_units`;
    /// - an unacknowledged AGGREGATE_HANDLES window is
    ///   `COORDINATOR_REASON_CONFLICT_UNRESOLVED`, no args;
    ///
    /// and a command given up at a cap, every write lost to a racing one,
    /// is `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED`, INTERNAL (a fatal bug's
    /// status; decision 14.46), args `attempts` and `cap`;
    ///
    /// and, each UNAVAILABLE (bug, retryable), so that when the retries are
    /// spent the key is what is reported (decision 14.51):
    ///
    /// - the store's conflict, a race lost at the write handed back at a
    ///   race re-run cap of 0, is `COORDINATOR_REASON_STORE_CONFLICT`, args
    ///   `expected` (the head stamped) and `actual` (the head found);
    /// - the store failing to answer is
    ///   `COORDINATOR_REASON_STORE_UNAVAILABLE`, the store's failure its
    ///   `cause` (and `cause.<arg>`);
    /// - an upcaster sidecar that could not be reached is
    ///   `COORDINATOR_REASON_UPCASTER_UNAVAILABLE`, args `domain` and
    ///   `cause`;
    /// - a handler that could not be reached is
    ///   `COORDINATOR_REASON_HANDLER_UNAVAILABLE` (decision 14.49), args
    ///   `domain` and `cause`.
    pub fn as_coded(&self) -> Option<angzarr_router::error::CodedError> {
        use super::reasons::{args, CoordinatorReason};
        use angzarr_router::error::{BusinessStatus, CodedError};
        let (reason, extras) = match self {
            KernelError::ExpectedSequenceAhead { expected, head, .. } => {
                return Some(CodedError::rejection(
                    BusinessStatus::InvalidArgument,
                    CoordinatorReason::ExpectedSequenceAhead.as_str_name(),
                    [
                        (args::EXPECTED.to_string(), expected.to_string()),
                        (args::HEAD.to_string(), head.to_string()),
                    ],
                ));
            }
            KernelError::Stale {
                expected, actual, ..
            } => (
                CoordinatorReason::ExpectedSequenceStale,
                vec![
                    (args::EXPECTED.to_string(), expected.to_string()),
                    (args::ACTUAL.to_string(), actual.to_string()),
                ],
            ),
            KernelError::Overlap {
                window_units,
                command_units,
                ..
            } => (
                CoordinatorReason::ConflictOverlap,
                vec![
                    (args::WINDOW_UNITS.to_string(), window_units.clone()),
                    (args::COMMAND_UNITS.to_string(), command_units.clone()),
                ],
            ),
            KernelError::Unresolved { .. } => (CoordinatorReason::ConflictUnresolved, Vec::new()),
            KernelError::RetryCapExhausted { attempts, cap, .. } => {
                return Some(super::route::internal(
                    CoordinatorReason::RetryCapExhausted.as_str_name(),
                    [
                        (args::ATTEMPTS.to_string(), attempts.to_string()),
                        (args::CAP.to_string(), cap.to_string()),
                    ],
                ));
            }
            KernelError::RulesVersionMismatch {
                stream,
                registered,
                reported,
            } => {
                return Some(super::route::internal(
                    angzarr_proto::v1::HostReason::DescriptorRulesVersionMismatch.as_str_name(),
                    [
                        ("component".to_string(), stream.domain.clone()),
                        ("described".to_string(), registered.clone()),
                        ("reported".to_string(), reported.clone()),
                    ],
                ));
            }
            KernelError::ChainTooDeep { limit } => {
                return Some(super::route::internal(
                    CoordinatorReason::ChainTooDeep.as_str_name(),
                    [(args::LIMIT.to_string(), limit.to_string())],
                ));
            }
            KernelError::TooManySteps { limit, .. } => {
                return Some(super::route::internal(
                    super::route::TOO_MANY_STEPS,
                    [(args::LIMIT.to_string(), limit.to_string())],
                ));
            }
            KernelError::Store(StoreError::Conflict {
                expected, actual, ..
            }) => {
                return Some(unavailable(
                    CoordinatorReason::StoreConflict,
                    vec![
                        (args::EXPECTED.to_string(), expected.to_string()),
                        (args::ACTUAL.to_string(), actual.to_string()),
                    ],
                ));
            }
            KernelError::Store(error @ StoreError::Unavailable(_)) => {
                return Some(unavailable(
                    CoordinatorReason::StoreUnavailable,
                    super::route::flat_args(&store_report(error)),
                ));
            }
            KernelError::Upcast {
                stream,
                error: error @ UpcastError::Unavailable { .. },
            } => {
                let mut extras = vec![(args::DOMAIN.to_string(), stream.domain.clone())];
                extras.extend(super::route::flat_args(error));
                return Some(unavailable(CoordinatorReason::UpcasterUnavailable, extras));
            }
            KernelError::HandlerUnavailable { stream, cause } => {
                return Some(CodedError {
                    code: super::route::HANDLER_UNAVAILABLE.to_string(),
                    extras: [
                        (args::DOMAIN.to_string(), stream.domain.clone()),
                        (
                            angzarr_router::error::extras::CAUSE.to_string(),
                            cause.clone(),
                        ),
                    ]
                    .into(),
                    grpc: angzarr_router::error::GrpcCode::Unavailable,
                    refusal: None,
                });
            }
            _ => return None,
        };
        Some(angzarr_router::error::CodedError::immediate(
            reason.as_str_name(),
            extras,
        ))
    }

    pub fn recovery(&self) -> Recovery {
        match self {
            KernelError::Malformed(_)
            | KernelError::ExpectedSequenceAhead { .. }
            | KernelError::UnknownType { .. }
            | KernelError::InvalidPayload { .. } => Recovery::Invalid,
            KernelError::Stale { .. }
            | KernelError::Overlap { .. }
            | KernelError::Unresolved { .. } => Recovery::Retry,
            KernelError::Store(error) => store_recovery(error),
            KernelError::Router(error) => router_recovery(error),
            KernelError::HandlerUnavailable { .. }
            | KernelError::Upcast {
                error: UpcastError::Unavailable { .. },
                ..
            } => Recovery::Retry,
            KernelError::Upcast { .. }
            | KernelError::RetryCapExhausted { .. }
            | KernelError::RulesVersionMismatch { .. }
            | KernelError::WrongDomain { .. }
            | KernelError::ChainTooDeep { .. }
            | KernelError::TooManySteps { .. }
            | KernelError::BeforeSnapshot { .. }
            | KernelError::UnregisteredDomain { .. }
            | KernelError::Unreadable { .. }
            | KernelError::Replay(_) => Recovery::Bug,
        }
    }
}

/// `reason` under UNAVAILABLE, a retryable bug's status (decision 14.37),
/// `extras` its metadata.
fn unavailable(
    reason: super::reasons::CoordinatorReason,
    extras: Vec<(String, String)>,
) -> angzarr_router::error::CodedError {
    angzarr_router::error::CodedError {
        code: reason.as_str_name().to_string(),
        extras: extras.into_iter().collect(),
        grpc: angzarr_router::error::GrpcCode::Unavailable,
        refusal: None,
    }
}

/// A store's error as a report: angzarr-ports' code and args, as
/// [`KernelError::Store`] forwards them.
fn store_report(error: &StoreError) -> Report {
    store_args(error)
        .into_iter()
        .fold(Report::new(error.code()), |report, (name, arg)| {
            report.arg(name, arg)
        })
}

/// A stream as an error names it: `domain/root`, then `@edition` outside
/// the main timeline.
pub(crate) fn cover_text(cover: &Cover) -> String {
    let root = cover.root.as_ref().map(Uuid::to_string).unwrap_or_default();
    match &cover.edition {
        Some(edition) => format!("{}/{root}@{edition}", cover.domain),
        None => format!("{}/{root}", cover.domain),
    }
}

fn stream(cover: &Cover) -> Arg {
    cover_text(cover).into()
}

// ---------------------------------------------------------------- Failure

impl<R: Diagnostic> fmt::Display for Failure<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl<R: Diagnostic> Diagnostic for Failure<R> {
    fn code(&self) -> &str {
        match self {
            Failure::Rejected(rejection) => rejection.code(),
            Failure::Downstream { .. } => "angzarr-embedded-downstream",
            Failure::Kernel(error) => error.code(),
            Failure::Projection { .. } => "angzarr-embedded-projection-failed",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Failure::Rejected(rejection) => rejection.args(),
            Failure::Downstream { path, error } => {
                vec![("depth", path.len().into()), ("error", Arg::nested(error))]
            }
            Failure::Kernel(error) => error.args(),
            Failure::Projection { failures, .. } => {
                let mut args = vec![("failures", failures.len().into())];
                if let Some((projector, error)) = failures.first() {
                    args.push(("projector", projector.as_str().into()));
                    args.push(("error", Arg::nested(error)));
                }
                args
            }
        }
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for StepError {
    fn code(&self) -> &str {
        match self {
            StepError::Rejected(rejection) | StepError::PolicyFailed(rejection) => rejection.code(),
            StepError::Kernel(error) => error.code(),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            StepError::Rejected(rejection) | StepError::PolicyFailed(rejection) => rejection.args(),
            StepError::Kernel(error) => error.args(),
        }
    }
}

// ---------------------------------------------------------------- KernelError

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for KernelError {
    fn code(&self) -> &str {
        match self {
            KernelError::Malformed(m) => m.code(),
            KernelError::Stale { .. } => "angzarr-embedded-stale",
            KernelError::ExpectedSequenceAhead { .. } => "angzarr-embedded-expected-sequence-ahead",
            KernelError::Unresolved { .. } => "angzarr-embedded-unresolved",
            KernelError::RetryCapExhausted { .. } => "angzarr-embedded-retry-cap-exhausted",
            KernelError::RulesVersionMismatch { .. } => "angzarr-embedded-rules-version-mismatch",
            KernelError::Overlap { .. } => "angzarr-embedded-overlap",
            KernelError::UnknownType { .. } => "angzarr-embedded-unknown-type",
            KernelError::InvalidPayload { .. } => "angzarr-embedded-invalid-payload",
            KernelError::Upcast { .. } => "angzarr-embedded-upcast",
            KernelError::WrongDomain { .. } => "angzarr-embedded-wrong-domain",
            KernelError::ChainTooDeep { .. } => "angzarr-embedded-chain-too-deep",
            KernelError::TooManySteps { .. } => "angzarr-embedded-step-limit",
            KernelError::BeforeSnapshot { .. } => "angzarr-embedded-before-snapshot",
            KernelError::UnregisteredDomain { .. } => "angzarr-embedded-unregistered-domain",
            KernelError::Replay(error) => error.code(),
            KernelError::Unreadable { .. } => "angzarr-embedded-unreadable",
            KernelError::Store(error) => error.code(),
            KernelError::Router(error) => router_diagnostic(error).0,
            KernelError::HandlerUnavailable { .. } => "angzarr-embedded-handler-unavailable",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            KernelError::Malformed(m) => m.args(),
            KernelError::Stale {
                stream: s,
                expected,
                actual,
            }
            | KernelError::Unresolved {
                stream: s,
                expected,
                actual,
            } => vec![
                ("stream", stream(s)),
                ("expected", (*expected).into()),
                ("actual", (*actual).into()),
            ],
            KernelError::ExpectedSequenceAhead {
                stream: s,
                expected,
                head,
            } => vec![
                ("stream", stream(s)),
                ("expected", (*expected).into()),
                ("head", (*head).into()),
            ],
            KernelError::Overlap {
                stream: s,
                expected,
                actual,
                window_units,
                command_units,
            } => vec![
                ("stream", stream(s)),
                ("expected", (*expected).into()),
                ("actual", (*actual).into()),
                ("window_units", window_units.as_str().into()),
                ("command_units", command_units.as_str().into()),
            ],
            KernelError::RetryCapExhausted {
                stream: s,
                attempts,
                cap,
            } => vec![
                ("stream", stream(s)),
                ("attempts", (*attempts).into()),
                ("cap", (*cap).into()),
            ],
            KernelError::RulesVersionMismatch {
                stream: s,
                registered,
                reported,
            } => vec![
                ("stream", stream(s)),
                ("registered", registered.as_str().into()),
                ("reported", reported.as_str().into()),
            ],
            KernelError::UnknownType {
                stream: s,
                type_url,
            } => vec![
                ("stream", stream(s)),
                ("type_url", type_url.as_str().into()),
            ],
            KernelError::InvalidPayload {
                stream: s,
                type_url,
                reason,
            } => vec![
                ("stream", stream(s)),
                ("type_url", type_url.as_str().into()),
                ("reason", Arg::nested(reason)),
            ],
            KernelError::Upcast { stream: s, error } => {
                vec![("stream", stream(s)), ("error", Arg::nested(error))]
            }
            KernelError::WrongDomain {
                stream: s,
                expected,
            } => {
                vec![("stream", stream(s)), ("expected", (*expected).into())]
            }
            KernelError::ChainTooDeep { limit } => vec![("limit", (*limit).into())],
            KernelError::TooManySteps { stream: s, limit } => {
                vec![("stream", stream(s)), ("limit", (*limit).into())]
            }
            KernelError::BeforeSnapshot { stream: s, first } => {
                vec![("stream", stream(s)), ("first", (*first).into())]
            }
            KernelError::UnregisteredDomain { domain } => {
                vec![("domain", domain.as_str().into())]
            }
            KernelError::Replay(error) => error.args(),
            KernelError::Unreadable {
                stream: s,
                sequence,
                reason,
            } => vec![
                ("stream", stream(s)),
                ("sequence", (*sequence).into()),
                ("reason", Arg::nested(reason)),
            ],
            KernelError::Store(error) => store_args(error),
            KernelError::Router(error) => router_diagnostic(error).1,
            KernelError::HandlerUnavailable { stream: s, cause } => {
                vec![("stream", stream(s)), ("cause", cause.as_str().into())]
            }
        }
    }
}

/// The four classes of error (decision 14.37), decided by gRPC status
/// alone, and by the kernel itself (decision 14.42): the router answers a
/// status and a key and never classifies.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Class {
    /// FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS,
    /// OUT_OF_RANGE, PERMISSION_DENIED: a refusal, final; to the caller, or
    /// the issuing policy's OnRefused; never retried.
    BusinessFatal,
    /// ABORTED: re-read and re-decide. Returned at once to an outside
    /// caller; a policy step, whose caller is the kernel, is run again.
    BusinessImmediate,
    /// INTERNAL, UNKNOWN, DATA_LOSS: dead-lettered and alerted at once.
    BugFatal,
    /// Every other status (UNAVAILABLE, DEADLINE_EXCEEDED,
    /// RESOURCE_EXHAUSTED, UNIMPLEMENTED, CANCELLED, ...): retried under the
    /// capped backoff, then dead-lettered.
    BugRetryable,
}

/// The class of the gRPC status `code`, by its wire number.
pub fn class(code: i32) -> Class {
    match code {
        3 | 5 | 6 | 7 | 9 | 11 => Class::BusinessFatal,
        10 => Class::BusinessImmediate,
        2 | 13 | 15 => Class::BugFatal,
        _ => Class::BugRetryable,
    }
}

/// What to do about the router's error: its status decides its class
/// ([`class`], decision 14.37). A business refusal is
/// the caller's to hear, a fatal bug an operator's; ABORTED (reload) and a
/// retryable bug are run again.
fn router_recovery(error: &angzarr_router::error::CodedError) -> Recovery {
    match class(error.grpc as i32) {
        Class::BusinessFatal => Recovery::Business,
        Class::BugFatal => Recovery::Bug,
        Class::BusinessImmediate | Class::BugRetryable => Recovery::Retry,
    }
}

/// A router error's code and args, as the kernel's: the router's reason
/// under angzarr-embedded's code ([`reason_code`](super::route::reason_code)),
/// its metadata the args; a handler's own reason, which no catalog of
/// angzarr-embedded's holds, as an unhandled handler error with that reason
/// its `cause` and its metadata under `cause.<name>`, as the router answers
/// a handler's bug.
fn router_diagnostic(error: &angzarr_router::error::CodedError) -> (&str, Vec<(&str, Arg)>) {
    match super::route::reason_code(&error.code) {
        Some(code) => (code, super::route::router_args(error)),
        None => {
            let cause = error
                .extras
                .iter()
                .fold(Report::new(error.code.clone()), |cause, (name, value)| {
                    cause.arg(name.clone(), value.as_str())
                });
            let args = vec![("cause", Arg::Nested(Box::new(cause)))];
            (
                super::route::reason_code(
                    angzarr_router::error::RouterReason::UnhandledHandlerError.as_str_name(),
                )
                .unwrap_or_default(),
                args,
            )
        }
    }
}

// ---------------------------------------------------------------- ReplayError

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for ReplayError {
    fn code(&self) -> &str {
        match self {
            ReplayError::OutOfSequence { .. } => "angzarr-embedded-replay-out-of-sequence",
            ReplayError::RecordedBackwards { .. } => "angzarr-embedded-replay-recorded-backwards",
            ReplayError::IncompleteMeta { .. } => "angzarr-embedded-replay-incomplete-meta",
            ReplayError::WrongDomain { .. } => "angzarr-embedded-replay-wrong-domain",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            ReplayError::OutOfSequence { index, found } => {
                vec![("index", (*index).into()), ("found", (*found).into())]
            }
            ReplayError::RecordedBackwards { sequence } => vec![("sequence", (*sequence).into())],
            ReplayError::IncompleteMeta { sequence, field } => {
                vec![("sequence", (*sequence).into()), ("field", (*field).into())]
            }
            ReplayError::WrongDomain { found, expected } => vec![
                ("found", found.as_str().into()),
                ("expected", (*expected).into()),
            ],
        }
    }
}

// ---------------------------------------------------------------- StoreError

/// What to do about a store's error: angzarr-ports' [`Recovery`]
/// (`angzarr_ports::Recovery`), one to one (`Retry`, `Invalid`, `Bug`).
pub(crate) fn store_recovery(error: &StoreError) -> Recovery {
    match error.recovery() {
        angzarr_ports::Recovery::Retry => Recovery::Retry,
        angzarr_ports::Recovery::Invalid => Recovery::Invalid,
        angzarr_ports::Recovery::Bug => Recovery::Bug,
    }
}

/// A store error's args as angzarr-embedded's diagnostics carry them: angzarr-ports'
/// args by name, unchanged (`StoreError::args`), each converted by
/// angzarr-ports' own `From` (a `Uint` as a `Uint`, a `Str` as a `Str`, and
/// a wrapped `Cause`, the adapter's own code and args, as a nested
/// diagnostic under that code).
pub(crate) fn store_args(error: &StoreError) -> Vec<(&'static str, Arg)> {
    error
        .args()
        .into_iter()
        .map(|(name, arg)| (name, arg.into()))
        .collect()
}

/// A stored page the kernel cannot give back as a current event: its
/// payload upcasts or decodes as nothing the domain knows, or its bytes do
/// not decode as that type. The kernel's, never the store's (angzarr-ports'
/// `StoreError::Unreadable` is the store's: bytes it cannot return).
pub(crate) fn unreadable(stream: &Cover, sequence: u64, reason: Report) -> KernelError {
    KernelError::Unreadable {
        stream: stream.clone(),
        sequence,
        reason,
    }
}

// ---------------------------------------------------------------- payloads

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for PayloadError {}

impl Diagnostic for PayloadError {
    fn code(&self) -> &str {
        match self {
            PayloadError::UnknownType { .. } => "angzarr-embedded-payload-unknown-type",
            PayloadError::Decode { .. } => "angzarr-embedded-payload-decode",
            PayloadError::Invalid { .. } => "angzarr-embedded-payload-invalid",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            PayloadError::UnknownType { type_url } | PayloadError::Decode { type_url, .. } => {
                vec![("type_url", type_url.as_str().into())]
            }
            PayloadError::Invalid { type_url, reason } => vec![
                ("type_url", type_url.as_str().into()),
                ("reason", Arg::nested(reason)),
            ],
        }
    }
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for UpcastError {
    fn code(&self) -> &str {
        match self {
            UpcastError::Payload(error) => error.code(),
            UpcastError::Rule { .. } => "angzarr-embedded-upcast-rule",
            UpcastError::Unavailable { .. } => "angzarr-embedded-upcast-unavailable",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            UpcastError::Payload(error) => error.args(),
            UpcastError::Rule { source, error } => vec![
                ("source", source.as_str().into()),
                ("error", Arg::nested(error)),
            ],
            UpcastError::Unavailable { cause } => vec![("cause", Arg::nested(cause))],
        }
    }
}

impl fmt::Display for UpcastChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for UpcastChainError {
    fn code(&self) -> &str {
        match self.problem {
            ChainProblem::Duplicate { .. } => "angzarr-embedded-upcaster-duplicate",
            ChainProblem::OutOfOrder { .. } => "angzarr-embedded-upcaster-out-of-order",
            ChainProblem::DeadEnd { .. } => "angzarr-embedded-upcaster-dead-end",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let mut args = vec![("domain", self.domain.as_str().into())];
        match &self.problem {
            ChainProblem::Duplicate { source } => args.push(("source", source.as_str().into())),
            ChainProblem::OutOfOrder {
                source,
                produced_by,
            } => {
                args.push(("source", source.as_str().into()));
                args.push(("produced_by", produced_by.as_str().into()));
            }
            ChainProblem::DeadEnd { source, ends_at } => {
                args.push(("source", source.as_str().into()));
                args.push(("ends_at", ends_at.as_str().into()));
            }
        }
        args
    }
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;

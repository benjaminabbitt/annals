//! Where the kernel meets angzarr-router: the router owns business
//! dispatch (handlers, typed refusals and the refusal path); this module turns what the router answers into the kernel's terms.
//!
//! - [`reason_code`]: every reason the router answers with, and every
//!   coordinator key the kernel answers in the router's shape, each
//!   forwarded under an angzarr-embedded code of its own with its own
//!   message ([`KernelError::Router`](super::KernelError::Router)).
//! - [`Rejected`]: a domain's refusal, as the caller receives it: the
//!   refusing handler's code and args, and its typed refusal object.
//! - The table a typed [`Domain`] is dispatched by: the router's
//!   `DomainDispatch` over the domain's state, folding with the domain's
//!   own `apply`, with the state's codec,
//!   resolving COMMUTATIVE by the state's conflict units; the domain's
//!   handlers are registered on it.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use angzarr_diagnostic::{Arg, Diagnostic, Report};
use angzarr_proto::v1::ConflictResolution;
use angzarr_router::domain::DomainDispatch;
use angzarr_router::error::{BusinessStatus, CodedError, GrpcCode, HandlerError};
use angzarr_router::rebuild::{FoldFailure, Rebuilder};
use prost::Message;
use prost_types::Any;

use super::domain::{Domain, State};
use super::error::PayloadError;
use super::markup::Failed;
use super::payload::{type_name, Payload};

/// The angzarr-embedded code a reason in the router's shape is forwarded
/// under, for every reason angzarr-router answers with (each `RouterReason`
/// but `UNSPECIFIED`) and every coordinator key the kernel answers in that
/// shape ([`reasons::ANSWERED`](super::reasons::ANSWERED)):
/// `angzarr-embedded-next-` and the reason, its enum's prefix dropped, in
/// kebab-case (`COORDINATOR_REASON_PM_NO_INSTANCE` is
/// `angzarr-embedded-next-pm-no-instance`); `None` for a reason that is
/// neither (a domain's own). The catalog check holds the catalog to the
/// router's list.
pub(crate) fn reason_code(reason: &str) -> Option<&'static str> {
    static CODES: OnceLock<BTreeMap<&'static str, String>> = OnceLock::new();
    CODES
        .get_or_init(|| forwarded().map(|known| (known, kebab(known))).collect())
        .get(reason)
        .map(String::as_str)
}

/// Every reason [`reason_code`] forwards, by its full name: the router's,
/// then the coordinator's the kernel answers.
pub(crate) fn forwarded() -> impl Iterator<Item = &'static str> {
    (super::reasons::router().map(|reason| reason.as_str_name())).chain(
        super::reasons::ANSWERED
            .iter()
            .map(|reason| reason.as_str_name()),
    )
}

/// `PREFIX_REASON` as `angzarr-embedded-next-reason`.
fn kebab(reason: &str) -> String {
    format!(
        "angzarr-embedded-next-{}",
        super::reasons::unprefixed(reason)
            .to_ascii_lowercase()
            .replace('_', "-")
    )
}

/// `error`'s metadata as args, by name, each as text: angzarr-router's
/// keys, a wrapped cause already flattened (`cause`, `cause.<arg>`). The
/// names are the error's own, borrowed: what a handler answers is held
/// only as long as its error.
pub(crate) fn router_args(error: &CodedError) -> Vec<(&str, Arg)> {
    error
        .extras
        .iter()
        .map(|(key, value)| (key.as_str(), Arg::Str(value.clone())))
        .collect()
}

/// The router's key for a refusal past a decision's step limit
/// ([`STEP_LIMIT`](super::markup::STEP_LIMIT)), the spec's
/// `ROUTER_REASON_TOO_MANY_STEPS` (decision 14.49), with arg `limit`: the
/// kernel's [`KernelError::TooManySteps`](super::KernelError::TooManySteps).
pub(crate) const TOO_MANY_STEPS: &str = "ROUTER_REASON_TOO_MANY_STEPS";

/// A domain's refusal of a command, as its caller receives it (angzarr's
/// business rejection, [`is_rejection`]): the code and args of the
/// refusal the handler made (or the router's, a process manager's
/// `COORDINATOR_REASON_PM_NO_INSTANCE`), and the typed refusal object it carries, if any.
/// The refusing domain recorded nothing.
#[derive(Clone, PartialEq, Debug)]
pub struct Rejected {
    report: Report,
    key: String,
    refusal: Option<Any>,
    status: BusinessStatus,
}

impl Rejected {
    /// The router's rejection, in the kernel's terms: under the router's
    /// reason as angzarr-embedded's code (a typed refusal's
    /// ROUTER_REASON_COMMAND_REFUSED), or the handler's own, its metadata the args.
    pub(crate) fn of(error: &CodedError) -> Rejected {
        let code = match reason_code(&error.code) {
            Some(code) => std::borrow::Cow::Borrowed(code),
            None => std::borrow::Cow::Owned(error.code.clone()),
        };
        let report = router_args(error)
            .into_iter()
            .fold(Report::new(code), |report, (name, arg)| {
                report.arg(name.to_string(), arg)
            });
        Rejected {
            report,
            key: error.code.clone(),
            refusal: error.refusal.as_deref().cloned(),
            status: BusinessStatus::try_from(error.grpc)
                .unwrap_or(BusinessStatus::FailedPrecondition),
        }
    }

    /// The refusal as a report: its code and args.
    pub fn report(&self) -> &Report {
        &self.report
    }

    /// The refusal as the wire carries it: the refusing handler's key (a
    /// typed refusal's ROUTER_REASON_COMMAND_REFUSED), not angzarr-embedded's
    /// code for it, and its args.
    pub(crate) fn wire(&self) -> angzarr_proto::v1::Diagnostic {
        angzarr_proto::v1::Diagnostic {
            code: self.key.clone(),
            args: flat_args(&self.report).into_iter().collect(),
        }
    }

    /// The business status the refusal went out under.
    pub fn status(&self) -> BusinessStatus {
        self.status
    }

    /// As a refused policy receives it, angzarr's `RefusalResponse`: the
    /// typed refusal object, or an untyped refusal as the wire carries it
    /// ([`Rejected::wire`]): the key it went out under, the handler's own
    /// or the spec's, never angzarr-embedded's code for it, and its args.
    pub fn response(&self) -> angzarr_proto::v1::RefusalResponse {
        use angzarr_proto::v1::refusal_response::Reason;
        angzarr_proto::v1::RefusalResponse {
            reason: Some(match &self.refusal {
                Some(object) => Reason::Refusal(object.clone()),
                None => Reason::Diagnostic(self.wire()),
            }),
        }
    }

    /// The typed refusal object, packed, if the handler refused with one.
    pub fn refusal_object(&self) -> Option<&Any> {
        self.refusal.as_ref()
    }

    /// The typed refusal object, as `T`, if the handler refused with one
    /// and it is a `T`.
    pub fn refusal<T: super::markup::DomainMessage>(&self) -> Option<T> {
        super::markup::decode_message(self.refusal.as_ref()?)
            .ok()
            .flatten()
    }
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.report.fmt(f)
    }
}

impl Diagnostic for Rejected {
    fn code(&self) -> &str {
        self.report.code()
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        self.report.args()
    }
}

/// Whether the router's `error` is a business refusal: its status is of
/// the business set (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
/// ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED), which decides the
/// class (decision 14.37). The router answers its own errors on malformed
/// input INTERNAL, so a refusal is a handler's, or the router's on its
/// behalf (a typed refusal's ROUTER_REASON_COMMAND_REFUSED, a process manager's
/// COORDINATOR_REASON_PM_NO_INSTANCE).
pub(crate) fn is_rejection(error: &CodedError) -> bool {
    super::error::class(error.grpc as i32) == super::error::Class::BusinessFatal
}

/// Whether the router's `error`, which the kernel keeps as
/// [`KernelError::Router`](super::KernelError::Router) (never a refusal,
/// [`is_rejection`]: that is [`Rejected`]), is a fatal bug ([`class`](super::class):
/// INTERNAL, UNKNOWN, DATA_LOSS), never a policy's failure
/// ([`POLICY_FAILED`]). A handler's bug is ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its
/// own code and args its `cause`; the router's own errors on malformed
/// input are bugs too. The kernel dead-letters it by its code and args at
/// once, and a policy's step moves the policy's position past it unless
/// the kernel says to stay ([`OnPolicyBug`](super::OnPolicyBug)). A
/// retryable bug is retried first, then dead-lettered.
pub(crate) fn is_handler_fault(error: &CodedError) -> bool {
    error.code != POLICY_FAILED
        && super::error::class(error.grpc as i32) == super::error::Class::BugFatal
}

/// A handler's refusal `rejection`, with its typed refusal object, as the
/// router carries it: a business rejection (FAILED_PRECONDITION). A typed
/// refusal is keyed ROUTER_REASON_COMMAND_REFUSED, its object's type as `refusal_type`
/// and the object beside (decision 14.37); a refusal without one under its
/// own code, its args flattened as metadata.
pub fn rejection(rejection: &dyn Diagnostic, refusal: Option<Any>) -> HandlerError {
    refused(rejection, refusal, BusinessStatus::FailedPrecondition)
}

/// A marked-up handler's failure, as the router carries it: a refusal
/// under its status ([`refused`]), an immediate return (ABORTED under its
/// own code, angzarr-router's `CodedError::immediate`), a fatal bug
/// ([`bug`]), or a retryable bug under the status it picked
/// (ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its cause as a fatal bug's,
/// `CodedError::retryable_bug`).
pub(crate) fn failed<R: Diagnostic>(failed: Failed<super::markup::Rejection<R>>) -> HandlerError {
    match failed {
        Failed::Rejected { rejection, status } => {
            refused(&rejection, rejection.refusal().cloned(), status)
        }
        Failed::Immediate(report) => {
            HandlerError::Coded(CodedError::immediate(report.code(), flat_args(&report)))
        }
        Failed::Bug(report) => bug(&report),
        Failed::RetryableBug { status, report } => HandlerError::Coded(CodedError::retryable_bug(
            status,
            report.code(),
            flat_args(&report),
        )),
    }
}

/// A handler's bug, as the router carries it (decision 14.37): INTERNAL
/// ROUTER_REASON_UNHANDLED_HANDLER_ERROR, the bug's code as its `cause` and its args,
/// flattened, as `cause.<arg>`; never a refusal.
pub(crate) fn bug(bug: &dyn Diagnostic) -> HandlerError {
    HandlerError::Coded(CodedError::bug(bug.code(), flat_args(bug)))
}

/// A handler's refusal `rejection` under `status`, with its typed refusal
/// object: a typed refusal keyed ROUTER_REASON_COMMAND_REFUSED, its object's type as
/// `refusal_type` and the object beside; a refusal without one under its
/// own code, its args flattened as metadata. Either goes out under
/// `status`.
fn refused(
    rejection: &dyn Diagnostic,
    refusal: Option<Any>,
    status: BusinessStatus,
) -> HandlerError {
    HandlerError::Coded(match refusal {
        Some(object) => CodedError {
            grpc: status.into(),
            ..CodedError::refused(object)
        },
        None => CodedError::rejection(status, rejection.code(), flat_args(rejection)),
    })
}

/// One of angzarr-embedded's own failures, INTERNAL under its own `code`
/// ([`POLICY_FAILED`], [`TOO_MANY_STEPS`], an upcast's failure): never a
/// handler's bug, which angzarr-router's `CodedError::bug` answers as
/// ROUTER_REASON_UNHANDLED_HANDLER_ERROR with the handler's code as its cause.
pub(crate) fn internal(
    code: &str,
    extras: impl IntoIterator<Item = (String, String)>,
) -> CodedError {
    CodedError {
        code: code.to_string(),
        extras: extras.into_iter().collect(),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}

/// The router's code for a policy's own failure (its `on_event`'s error, a
/// marked event that does not decode, or its source's `#[on_refused]`
/// handler's error): the kernel's
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed), a defect.
pub(crate) const POLICY_FAILED: &str = "ANGZARR_EMBEDDED_POLICY_FAILED";

/// The metadata key a policy's failure carries its own code under: no arg
/// name has a hyphen, so it never meets one of the failure's args.
const POLICY_REASON: &str = "policy-reason";

/// A diagnostic's args, flattened as angzarr carries them (decision
/// 14.37): each by its name, a wrapped cause as its code under the arg's
/// name and each of its args under `<name>.<arg>`.
pub(crate) fn flat_args(diagnostic: &dyn Diagnostic) -> Vec<(String, String)> {
    let mut flat = Vec::new();
    for (name, arg) in diagnostic.args() {
        match arg {
            Arg::Int(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Uint(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Str(s) => flat.push((name.to_string(), s)),
            Arg::Nested(cause) => {
                flat.push((name.to_string(), cause.code().to_string()));
                flat.extend(
                    flat_args(cause.as_ref())
                        .into_iter()
                        .map(|(inner, value)| (format!("{name}.{inner}"), value)),
                );
            }
        }
    }
    flat
}

/// `diagnostic` as angzarr's wire `Diagnostic`: its code and its args,
/// flattened ([`flat_args`]).
pub(crate) fn diagnostic(diagnostic: &dyn Diagnostic) -> angzarr_proto::v1::Diagnostic {
    angzarr_proto::v1::Diagnostic {
        code: diagnostic.code().to_string(),
        args: flat_args(diagnostic).into_iter().collect(),
    }
}

/// A policy's `failure`, as the router carries it: INTERNAL under
/// [`POLICY_FAILED`], the failure's code and flattened args as metadata.
pub(crate) fn policy_failed(failure: &dyn Diagnostic) -> HandlerError {
    let extras = std::iter::once((POLICY_REASON.to_string(), failure.code().to_string()))
        .chain(flat_args(failure));
    HandlerError::Coded(internal(POLICY_FAILED, extras))
}

/// The policy's failure the router carries, if `error` is one: its code
/// and args, as [`policy_failed`] packed them.
pub(crate) fn policy_failure(error: &CodedError) -> Option<Report> {
    if error.code != POLICY_FAILED {
        return None;
    }
    let reason = error.extras.get(POLICY_REASON).cloned().unwrap_or_default();
    Some(
        error
            .extras
            .iter()
            .filter(|(name, _)| name.as_str() != POLICY_REASON)
            .fold(Report::new(reason), |report, (name, value)| {
                report.arg(name.clone(), value.as_str())
            }),
    )
}

/// The type URL of the page a fact handler that records no event answers
/// with: the router's fact record always holds one payload. Never written
/// and never folded.
pub const NOTHING: &str = "/angzarr_embedded.next.Nothing";

/// Whether `page` is a fact's record of nothing ([`NOTHING`]).
pub(crate) fn is_nothing(page: &angzarr_proto::v1::EventPage) -> bool {
    page.payload.as_ref().is_some_and(|p| p.type_url == NOTHING)
}

// ------------------------------------------------------------- the table

/// The key a remote handler is answered with when it cannot be reached
/// (angzarr-embedded-host's), the spec's
/// `COORDINATOR_REASON_HANDLER_UNAVAILABLE` (decision 14.49): the kernel's
/// retryable
/// [`KernelError::HandlerUnavailable`](super::KernelError::HandlerUnavailable).
pub(crate) const HANDLER_UNAVAILABLE: &str = "COORDINATOR_REASON_HANDLER_UNAVAILABLE";

/// The router's table for typed domain `D`, before its handlers: named
/// `D::NAME` (a process manager's if `process_manager`), folding each
/// event, upcast by the kernel before it is handed over, through `D::apply`,
/// answering `D::RULES_VERSION`, with the state's codec when it has a snapshot form
/// (a delivered starting state restored onto a fresh state from `fresh`,
/// and the state every answer carries), resolving COMMUTATIVE by
/// [`State::changed`] when the state supports it.
pub(crate) fn table<D: Domain>(
    fresh: Arc<dyn Fn() -> D::State + Send + Sync>,
    process_manager: bool,
) -> DomainDispatch<D::State> {
    let snapshot_form = <<D::State as State>::Snapshot as prost::Name>::full_name();
    let restore_fresh = Arc::clone(&fresh);
    let mut rebuilder = Rebuilder::new(move || fresh())
        .with_fold(move |state, pages, _cover| {
            for (index, page) in pages.iter().enumerate() {
                if is_nothing(page) {
                    continue;
                }
                let failed = |cause: &dyn Diagnostic| FoldFailure {
                    page_index: Some(index),
                    cause: Some(diagnostic(cause)),
                };
                let any = page.payload.clone().unwrap_or_default();
                let event = D::Event::from_any(&any).map_err(|e| failed(&e))?;
                D::apply(state, &event);
            }
            Ok(())
        })
        .with_state_type(&snapshot_form);
    if <D::State as State>::SNAPSHOTS {
        rebuilder = rebuilder.with_state_codec(
            move |any| {
                if type_name(&any.type_url) != snapshot_form {
                    return Err(Box::new(PayloadError::UnknownType {
                        type_url: any.type_url.clone(),
                    }));
                }
                let wire =
                    <D::State as State>::Snapshot::decode(&any.value[..]).map_err(|error| {
                        PayloadError::Decode {
                            type_url: any.type_url.clone(),
                            error,
                        }
                    })?;
                let mut restored = restore_fresh();
                restored.restore(wire).map_err(Box::new)?;
                Ok(restored)
            },
            |state: &D::State| super::payload::pack(&state.to_snapshot()),
        );
    }
    let base = match process_manager {
        true => DomainDispatch::process_manager(D::NAME, D::NAME, rebuilder),
        false => DomainDispatch::new(D::NAME, D::NAME, rebuilder),
    };
    let base = base.with_rules_version(D::RULES_VERSION);
    let commutative = <D::State as State>::descriptor().options.is_some_and(|o| {
        o.conflict_resolutions
            .contains(&(ConflictResolution::Commutative as i32))
    });
    match commutative {
        true => base.with_commutative_via(
            |state: &D::State| Ok(state.clone()),
            |state: &mut D::State, image: &D::State| {
                *state = image.clone();
                Ok(())
            },
            |before: &D::State, after: &D::State| {
                Ok(D::State::changed(before, after)
                    .iter()
                    .map(ToString::to_string)
                    .collect())
            },
        ),
        false => base,
    }
}

#[cfg(test)]
#[path = "route.test.rs"]
mod tests;

//! The coordinator's reason keys (decisions 14.37 and 14.44): the values of
//! angzarr-project's `CoordinatorReason` (`proto/io/angzarr/v1/reasons.proto`),
//! which the kernel answers, being the coordinator, and the names of their
//! args. A key travels as its value's full name, prefix included
//! (`COORDINATOR_REASON_PM_NO_INSTANCE`, from `as_str_name`). The router's
//! keys are angzarr-router's own (`RouterReason`); none is defined twice.
//! Each value's comment in `reasons.proto` is its entry: its status and its
//! args.

pub use angzarr_proto::v1::{CoordinatorReason, RouterReason};

/// The coordinator's keys an input is answered with, in the router's shape
/// (a code, its args as metadata, a status): what the kernel forwards as
/// [`KernelError::Router`](super::KernelError::Router) or a
/// [`Rejected`](super::Rejected), each under an angzarr-embedded code of its
/// own. The readiness keys (`COORDINATOR_REASON_MISSING_RULES_VERSION` and the rest) are a
/// component's, refused before any input.
/// `COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE` and
/// `COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD` are not among them: no
/// handler answers them; they are the kernel's own
/// [`KernelError::Stale`](super::KernelError::Stale) and
/// [`KernelError::ExpectedSequenceAhead`](super::KernelError::ExpectedSequenceAhead)
/// on the wire ([`KernelError::as_coded`](super::KernelError::as_coded)).
pub const ANSWERED: &[CoordinatorReason] = &[
    CoordinatorReason::ConflictUnresolved,
    CoordinatorReason::ConflictOverlap,
    CoordinatorReason::PmNoInstance,
    CoordinatorReason::RefusalFactWrongStream,
    CoordinatorReason::PolicyCommandOutsideDestination,
    CoordinatorReason::PolicyCommandOutsideEdition,
    CoordinatorReason::PolicyCommandNoRoot,
    CoordinatorReason::PolicyCommandOtherCorrelation,
    CoordinatorReason::PolicyCommandWithHandling,
    CoordinatorReason::PolicyCommandOtherIdentity,
    CoordinatorReason::PolicyCommandIsFact,
    CoordinatorReason::PolicyCommandFactOrigin,
    CoordinatorReason::RefusalFactAddressed,
    CoordinatorReason::RefusalFactNoPage,
    CoordinatorReason::RefusalFactNotAFact,
    CoordinatorReason::RefusalFactIdentified,
];

/// Every key angzarr-router answers with: each `RouterReason` but proto3's
/// zero, `UNSPECIFIED`, which is never answered.
pub fn router() -> impl Iterator<Item = RouterReason> {
    (1..).map_while(|number| RouterReason::try_from(number).ok())
}

/// `key` (a full value name) without its enum's prefix, `ROUTER_REASON_`
/// or `COORDINATOR_REASON_`: the name angzarr-embedded's own codes are
/// derived from. A key with neither is as given.
pub fn unprefixed(key: &str) -> &str {
    ["ROUTER_REASON_", "COORDINATOR_REASON_"]
        .iter()
        .find_map(|prefix| key.strip_prefix(prefix))
        .unwrap_or(key)
}

/// The catalog's arg names for the coordinator's keys (the `ErrorInfo`
/// metadata keys) that angzarr-router's `error::extras` does not name.
pub mod args {
    /// A command's expected sequence (`CommandBook.handling.expected_sequence`).
    pub const EXPECTED: &str = "expected";
    /// The stream's head, its next sequence, where the command is stale.
    pub const ACTUAL: &str = "actual";
    /// The stream's head, its next sequence, where the command is ahead.
    pub const HEAD: &str = "head";
    /// How many times business code ran before a cap.
    pub const ATTEMPTS: &str = "attempts";
    /// The cap reached: a count of runs or deliveries.
    pub const CAP: &str = "cap";
    /// A limit reached: a chain's depth, or a decision's continuation steps
    /// (decision 14.49).
    pub const LIMIT: &str = "limit";
    /// The domain a refusal fact or a policy's command was addressed to.
    pub const DOMAIN: &str = "domain";
    /// The `CommandPage` field a policy's command set to a value other
    /// than its coordinator's stamp: `command_id` or `causation_id`.
    pub const FIELD: &str = "field";
    /// The window's conflict units, comma-separated.
    pub const WINDOW_UNITS: &str = "window_units";
    /// The command's conflict units, comma-separated.
    pub const COMMAND_UNITS: &str = "command_units";
    /// A policy's one destination domain.
    pub const EXPECTED_DOMAIN: &str = "expected_domain";
    /// The domain a policy's command named instead.
    pub const ACTUAL_DOMAIN: &str = "actual_domain";
    /// The trigger's edition, which a policy's commands go into.
    pub const EXPECTED_EDITION: &str = "expected_edition";
    /// The edition a policy's command named instead.
    pub const ACTUAL_EDITION: &str = "actual_edition";
}

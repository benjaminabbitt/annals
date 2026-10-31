//! Steps 1 and 2 of the pipeline: **validate** a [`CommandBook`] once, then
//! **identify** it.
//!
//! [`check`] is the one place a book is checked. It is domain-blind: it
//! knows no domain's types, only the [`Registry`]'s entry for
//! `cover.domain` (is it registered, and which conflict resolutions does
//! its state support, as angzarr's coordinator reads them off the
//! `StateDescriptor`). Everything it refuses is [`Malformed`], before any
//! id is minted, anything is looked up or any business code runs. What it
//! accepts is a [`Checked`]: the same book, its required parts taken out of
//! their prost `Option`s, so nothing downstream unwraps one.
//!
//! [`Checked::identify`] then assigns the ids, as angzarr's
//! `command_identity` region says, and yields an [`Identified`], whose
//! command and correlation ids are never absent. The order is in the
//! types: only a `Checked` can be identified, and only an `Identified`
//! deduplicated ([`Coordinator::deduplicate`](super::Coordinator::deduplicate)).
//!
//! **No causation from outside.** [`check`] is the client entry: a book
//! that sets `causation_id` is refused ([`Malformed::CausationFromOutside`]).
//! angzarr-embedded is in process, so no caller outside it can deliver a reaction;
//! the kernel's own chain builds its policies' books and checks them through
//! a crate-private entry that derives their ids from the trigger.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_proto::ids::{self, UuidError};
use angzarr_proto::v1::{
    command_page, event_meta, CommandBook, CommandPage, ConflictResolution, Cover, EventMeta,
    EventRef, FactOrigin, Handling, Uuid,
};
use prost_types::{Any, Timestamp};

use super::registry::Registry;

/// Why a book was refused before anything was minted, looked up or decided:
/// angzarr's INVALID_ARGUMENT for a client's book. For a policy's book,
/// built by the kernel, it is a bug: the chain fails and the dead-letter
/// log says so ([`DeadLetterLog`](super::DeadLetterLog)). Listed in the
/// order [`check`] tries them; the first that applies is the answer.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Malformed {
    /// The book has no cover.
    NoCover,
    /// `cover.domain` is not registered: angzarr-embedded cannot say which conflict
    /// resolutions it supports, nor decode anything for it.
    UnknownDomain { domain: String },
    /// The cover has no root.
    NoRoot,
    /// The root is not a UUID's 16 bytes.
    BadRoot { len: usize },
    /// `cover.edition` is set but empty: the main timeline is the unset
    /// edition, never `""`.
    EmptyEdition,
    /// The book has no page.
    NoPage,
    /// The page has neither a command nor a fact.
    NoBody,
    /// The command or fact has no payload.
    NoPayload,
    /// A set `command_id` that is not 16 bytes.
    BadCommandId { len: usize },
    /// A set `correlation_id` that is not 16 bytes.
    BadCorrelationId { len: usize },
    /// A client's book with a `causation_id`: only the kernel's own chain
    /// issues reactions (angzarr's PERMISSION_DENIED for a client).
    CausationFromOutside,
    /// A policy's book that names its own command, correlation or causation
    /// id (`field`, the first set, by its `CommandPage` name): the kernel
    /// derives all three from the trigger.
    IdOnPolicyCommand { field: &'static str },
    /// A policy's book with a fact: a reaction can never inject a fact.
    FactFromPolicy,
    /// A policy's book with handling: the target stamps its own head.
    HandlingOnPolicyCommand,
    /// The reaction's trigger cannot name a command id
    /// ([`ids::policy_command_id`]): no stream or root, a bad root, an
    /// empty edition or component.
    BadTrigger(UuidError),
    /// A fact without `fact_origin`.
    FactWithoutOrigin,
    /// A `fact_origin` whose `external_id` is empty: it is the fact's
    /// identity.
    EmptyExternalId,
    /// A command with a `fact_origin`.
    CommandWithFactOrigin,
    /// A fact whose `command_id` is set to anything but its derivation,
    /// [`ids::fact_command_id`] of its external id.
    FactCommandIdNotDerived,
    /// A fact with any handling field: a fact is never checked against a
    /// sequence nor conflict-resolved. An empty handling is unset, and fine.
    HandlingOnFact,
    /// A `conflict_resolution` value this build does not know.
    UnknownConflictResolution { value: i32 },
    /// A `conflict_resolution` the domain's state does not support (its
    /// registered `StateDescriptor`): COMMUTATIVE on a state with no
    /// conflict units, above all.
    UnsupportedConflictResolution {
        domain: String,
        requested: ConflictResolution,
    },
    /// A refusal fact (angzarr's `CommandRefused`) for a stream other than
    /// the one its trigger names, domain, root and edition: a refusal fact
    /// is accepted only by the exact stream of the policy's triggering
    /// event.
    RefusalMisaddressed,
}

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Malformed {
    fn code(&self) -> &str {
        match self {
            Malformed::NoCover => "angzarr-embedded-next-malformed-no-cover",
            Malformed::UnknownDomain { .. } => "angzarr-embedded-next-malformed-unknown-domain",
            Malformed::NoRoot => "angzarr-embedded-next-malformed-no-root",
            Malformed::BadRoot { .. } => "angzarr-embedded-next-malformed-bad-root",
            Malformed::EmptyEdition => "angzarr-embedded-next-malformed-empty-edition",
            Malformed::NoPage => "angzarr-embedded-next-malformed-no-page",
            Malformed::NoBody => "angzarr-embedded-next-malformed-no-body",
            Malformed::NoPayload => "angzarr-embedded-next-malformed-no-payload",
            Malformed::BadCommandId { .. } => "angzarr-embedded-next-malformed-bad-command-id",
            Malformed::BadCorrelationId { .. } => {
                "angzarr-embedded-next-malformed-bad-correlation-id"
            }
            Malformed::CausationFromOutside => {
                "angzarr-embedded-next-malformed-causation-from-outside"
            }
            Malformed::IdOnPolicyCommand { .. } => {
                "angzarr-embedded-next-malformed-id-on-policy-command"
            }
            Malformed::FactFromPolicy => "angzarr-embedded-next-malformed-fact-from-policy",
            Malformed::HandlingOnPolicyCommand => {
                "angzarr-embedded-next-malformed-handling-on-policy-command"
            }
            Malformed::BadTrigger(_) => "angzarr-embedded-next-malformed-bad-trigger",
            Malformed::FactWithoutOrigin => "angzarr-embedded-next-malformed-fact-without-origin",
            Malformed::EmptyExternalId => "angzarr-embedded-next-malformed-empty-external-id",
            Malformed::CommandWithFactOrigin => {
                "angzarr-embedded-next-malformed-command-with-fact-origin"
            }
            Malformed::FactCommandIdNotDerived => {
                "angzarr-embedded-next-malformed-fact-command-id-not-derived"
            }
            Malformed::HandlingOnFact => "angzarr-embedded-next-malformed-handling-on-fact",
            Malformed::UnknownConflictResolution { .. } => {
                "angzarr-embedded-next-malformed-unknown-conflict-resolution"
            }
            Malformed::UnsupportedConflictResolution { .. } => {
                "angzarr-embedded-next-malformed-unsupported-conflict-resolution"
            }
            Malformed::RefusalMisaddressed => {
                "angzarr-embedded-next-malformed-refusal-misaddressed"
            }
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Malformed::UnknownDomain { domain } => vec![("domain", domain.as_str().into())],
            Malformed::BadRoot { len }
            | Malformed::BadCommandId { len }
            | Malformed::BadCorrelationId { len } => vec![("len", (*len).into())],
            Malformed::BadTrigger(error) => {
                let reason = match error {
                    UuidError::Length(_) => "bad-root",
                    UuidError::Text => "text",
                    UuidError::DomainContainsColon => "domain-contains-colon",
                    UuidError::MissingTrigger => "missing-trigger",
                    UuidError::EmptyEdition => "empty-edition",
                    UuidError::EmptyComponent => "empty-component",
                };
                vec![("reason", reason.into())]
            }
            Malformed::IdOnPolicyCommand { field } => vec![("field", (*field).into())],
            Malformed::UnknownConflictResolution { value } => vec![("value", (*value).into())],
            Malformed::UnsupportedConflictResolution { domain, requested } => vec![
                ("domain", domain.as_str().into()),
                ("requested", requested.as_str_name().into()),
            ],
            _ => Vec::new(),
        }
    }
}

/// What the application supplies for one command and its whole chain: the
/// kernel has no clock and no randomness, so it stays deterministic.
#[derive(Clone, PartialEq, Debug)]
pub struct Sampled {
    /// The instant: every event of the chain is recorded at it, or at its
    /// stream's previous `recorded_at` if that is later.
    pub now: Timestamp,
    /// A fresh UUIDv7 (RFC 9562), minted by the application for this
    /// command: its command id if a client's command arrives without one
    /// (and so its correlation, if it supplies none). Unused otherwise.
    pub command_id: Uuid,
}

/// What a command's page carries, checked: a command, or a fact with its
/// source. A view of `CommandPage.body` with `fact_origin` attached where
/// it belongs, so a fact's source is never optional.
#[derive(Clone, PartialEq, Debug)]
pub enum Body {
    /// Asks the aggregate to decide; may be rejected.
    Command { payload: Any },
    /// Records something that already happened; never rejected.
    Fact { payload: Any, source: FactOrigin },
}

impl Body {
    /// The payload, either arm.
    pub fn payload(&self) -> &Any {
        match self {
            Body::Command { payload } | Body::Fact { payload, .. } => payload,
        }
    }

    pub fn is_fact(&self) -> bool {
        matches!(self, Body::Fact { .. })
    }

    /// A fact's source; `None` for a command.
    pub fn source(&self) -> Option<&FactOrigin> {
        match self {
            Body::Fact { source, .. } => Some(source),
            Body::Command { .. } => None,
        }
    }
}

/// Who handed the kernel a book.
#[derive(Clone, PartialEq, Debug)]
enum Entry {
    /// The application, on a caller's behalf.
    Client,
    /// A policy in the kernel's own chain, reacting to `trigger`. The ids
    /// are derived at check time, so identifying cannot fail.
    Policy {
        trigger: EventRef,
        correlation_id: Uuid,
        command_id: Uuid,
    },
    /// A policy's refusal fact, built by the chain: its id is the refused
    /// command's, its correlation that command's, and its causation the
    /// policy's trigger.
    Refusal {
        trigger: EventRef,
        correlation_id: Uuid,
        command_id: Uuid,
    },
}

/// A [`CommandBook`] that passed [`check`]. Every accessor that the book
/// had to provide is non-optional; the ones that stay `Option` are the
/// sender's genuine choices (an id to be assigned, handling left to
/// configuration).
#[derive(Clone, PartialEq, Debug)]
pub struct Checked {
    cover: Cover,
    root: Uuid,
    body: Body,
    command_id: Option<Uuid>,
    correlation_id: Option<Uuid>,
    expected_sequence: Option<u64>,
    conflict_resolution: Option<ConflictResolution>,
    entry: Entry,
    /// Whether the cover is a process manager's: an absent correlation is
    /// its root.
    process_manager: bool,
}

/// Validate a client's `book`: step 1, the client entry. Domain-blind; the
/// registry answers only whether `cover.domain` is registered and what its
/// state supports. Refuses, in [`Malformed`]'s order, every case angzarr's
/// `Handling` region lists as malformed, plus a causation (only the
/// kernel's chain issues policy commands).
pub fn check(book: CommandBook, registry: &Registry) -> Result<Checked, Malformed> {
    parse(book, registry, None)
}

/// Validate a policy's `book`, built by the kernel for the `index`-th
/// command `component` issued reacting to `trigger` (whose correlation is
/// `correlation_id`): the same checks as [`check`], except that the book
/// must name no ids of its own, no handling and no fact, and its command id
/// is derived here ([`ids::policy_command_id`]).
///
/// The id never names the reactor's rules version or code, so a re-run over
/// a handled trigger (catch-up, redelivery) re-derives the same id and
/// deduplicates (angzarr decision 13.4). Repairs are new commands.
pub(crate) fn check_policy_command(
    book: CommandBook,
    registry: &Registry,
    trigger: EventRef,
    component: &str,
    index: u32,
    correlation_id: Uuid,
) -> Result<Checked, Malformed> {
    let command_id =
        ids::policy_command_id(&trigger, component, index).map_err(Malformed::BadTrigger)?;
    parse(
        book,
        registry,
        Some(Entry::Policy {
            trigger,
            correlation_id,
            command_id,
        }),
    )
}

/// The chain's entry for a policy's refusal fact: a fact whose command id
/// is the refused command's (`command_id`), with that command's
/// correlation, caused by the policy's `trigger`, addressed to the
/// trigger's stream.
pub(crate) fn check_refusal(
    book: CommandBook,
    registry: &Registry,
    trigger: EventRef,
    correlation_id: Uuid,
    command_id: Uuid,
) -> Result<Checked, Malformed> {
    parse(
        book,
        registry,
        Some(Entry::Refusal {
            trigger,
            correlation_id,
            command_id,
        }),
    )
}

/// The one parse: `policy` is `None` for a client's book.
fn parse(
    book: CommandBook,
    registry: &Registry,
    policy: Option<Entry>,
) -> Result<Checked, Malformed> {
    let CommandBook {
        cover,
        page,
        handling,
    } = book;

    let cover = cover.ok_or(Malformed::NoCover)?;
    let Some(target) = registry.target(&cover.domain) else {
        return Err(Malformed::UnknownDomain {
            domain: cover.domain,
        });
    };
    // A remote domain's capabilities are its coordinator's to judge.
    let local = matches!(target, super::Target::Local(_));
    let root = cover.root.clone().ok_or(Malformed::NoRoot)?;
    if !root.is_well_formed() {
        return Err(Malformed::BadRoot {
            len: root.value.len(),
        });
    }
    if cover.edition.as_deref() == Some("") {
        return Err(Malformed::EmptyEdition);
    }

    let CommandPage {
        body,
        command_id,
        correlation_id,
        causation_id,
        fact_origin,
    } = page.ok_or(Malformed::NoPage)?;
    let (is_fact, payload) = match body.ok_or(Malformed::NoBody)? {
        command_page::Body::Command(command) => (false, command.payload),
        command_page::Body::Fact(fact) => (true, fact.payload),
    };
    let payload = payload.ok_or(Malformed::NoPayload)?;

    if let Some(id) = &command_id {
        if !id.is_well_formed() {
            return Err(Malformed::BadCommandId {
                len: id.value.len(),
            });
        }
    }
    if let Some(id) = &correlation_id {
        if !id.is_well_formed() {
            return Err(Malformed::BadCorrelationId {
                len: id.value.len(),
            });
        }
    }

    let handling = handling.unwrap_or_default();
    let handled = handling != Handling::default();
    let entry = match policy {
        None if causation_id.is_some() => return Err(Malformed::CausationFromOutside),
        None => Entry::Client,
        Some(_) if command_id.is_some() => {
            return Err(Malformed::IdOnPolicyCommand {
                field: "command_id",
            })
        }
        Some(_) if correlation_id.is_some() => {
            return Err(Malformed::IdOnPolicyCommand {
                field: "correlation_id",
            })
        }
        Some(_) if causation_id.is_some() => {
            return Err(Malformed::IdOnPolicyCommand {
                field: "causation_id",
            })
        }
        Some(entry @ Entry::Refusal { .. }) => entry,
        Some(_) if is_fact => return Err(Malformed::FactFromPolicy),
        Some(_) if handled => return Err(Malformed::HandlingOnPolicyCommand),
        Some(entry) => entry,
    };

    let body = if is_fact {
        let source = fact_origin.ok_or(Malformed::FactWithoutOrigin)?;
        if source.external_id.is_empty() {
            return Err(Malformed::EmptyExternalId);
        }
        if let Some(id) = &command_id {
            if *id != ids::fact_command_id(&source.external_id) {
                return Err(Malformed::FactCommandIdNotDerived);
            }
        }
        if handled {
            return Err(Malformed::HandlingOnFact);
        }
        Body::Fact { payload, source }
    } else {
        if fact_origin.is_some() {
            return Err(Malformed::CommandWithFactOrigin);
        }
        Body::Command { payload }
    };

    // An input to a process manager whose correlation is unset is given
    // its cover's root (decision 14.31); a set one is kept, whatever it is
    // (decision 14.36).
    let process_manager = registry.is_process_manager(&cover.domain);
    if let Body::Fact { payload, .. } = &body {
        if !refusal_addressed(payload, &cover) {
            return Err(Malformed::RefusalMisaddressed);
        }
    }

    let conflict_resolution = match handling.conflict_resolution {
        0 => None,
        value => {
            let requested = ConflictResolution::try_from(value)
                .map_err(|_| Malformed::UnknownConflictResolution { value })?;
            if local && !registry.supports(&cover.domain, requested) {
                return Err(Malformed::UnsupportedConflictResolution {
                    domain: cover.domain,
                    requested,
                });
            }
            Some(requested)
        }
    };

    Ok(Checked {
        cover,
        root,
        body,
        command_id,
        correlation_id,
        expected_sequence: handling.expected_sequence,
        conflict_resolution,
        entry,
        process_manager,
    })
}

/// Whether `payload`, if it is angzarr's `CommandRefused`, is for `cover`:
/// its trigger names exactly that stream. Any other payload is.
fn refusal_addressed(payload: &Any, cover: &Cover) -> bool {
    use prost::Message;
    let refused = <crate::markup::v1::CommandRefused as prost::Name>::full_name();
    if crate::payload::type_name(&payload.type_url) != refused {
        return true;
    }
    crate::markup::v1::CommandRefused::decode(&payload.value[..])
        .ok()
        .and_then(|refusal| refusal.trigger)
        .and_then(|trigger| trigger.stream)
        .is_some_and(|stream| stream == *cover)
}

impl Checked {
    /// The target stream, as sent: domain, root (16 bytes), edition (unset
    /// for main, never empty).
    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    pub fn domain(&self) -> &str {
        &self.cover.domain
    }

    /// The root: always present, always 16 bytes.
    pub fn root(&self) -> &Uuid {
        &self.root
    }

    /// `None`: the main timeline. Never `Some("")`.
    pub fn edition(&self) -> Option<&str> {
        self.cover.edition.as_deref()
    }

    pub fn body(&self) -> &Body {
        &self.body
    }

    /// The payload: always present.
    pub fn payload(&self) -> &Any {
        self.body.payload()
    }

    /// The sender's command id, 16 bytes when set. `None`: one is assigned
    /// at [`Checked::identify`].
    pub fn command_id(&self) -> Option<&Uuid> {
        self.command_id.as_ref()
    }

    /// The sender's correlation, 16 bytes when set. `None`: one is
    /// assigned at [`Checked::identify`].
    pub fn correlation_id(&self) -> Option<&Uuid> {
        self.correlation_id.as_ref()
    }

    /// `None`: stamped with the head at load. Never set on a fact or a
    /// reaction.
    pub fn expected_sequence(&self) -> Option<u64> {
        self.expected_sequence
    }

    /// The requested resolution, supported by the domain's state when set.
    /// `None` (UNSPECIFIED, or no handling): resolved from configuration at
    /// load ([`Registry::resolve`]).
    pub fn conflict_resolution(&self) -> Option<ConflictResolution> {
        self.conflict_resolution
    }

    /// Whether the kernel's own chain issued it.
    pub fn is_policy_command(&self) -> bool {
        matches!(self.entry, Entry::Policy { .. })
    }

    /// Step 2: assign the ids (angzarr's `command_identity` region).
    ///
    /// - A client's command: `command_id` as sent, else `sampled.command_id`
    ///   (the application's UUIDv7); correlation as sent, else the command
    ///   id.
    /// - A fact: `command_id` = [`ids::fact_command_id`] of its external id
    ///   (a sent one was checked equal); correlation as sent, else
    ///   [`ids::fact_correlation`] (the same value).
    /// - A reaction: the command id derived from its trigger, the trigger's
    ///   correlation, and the trigger as the command's own causation
    ///   (`CommandPage.causation_id`; never copied onto its events).
    ///
    /// Infallible: everything that could make it fail was refused by the
    /// check.
    pub fn identify(self, sampled: &Sampled) -> Identified {
        let Checked {
            cover,
            root,
            body,
            command_id,
            correlation_id,
            expected_sequence,
            conflict_resolution,
            entry,
            process_manager,
        } = self;
        // A reaction's correlation is its trigger's: the kernel's, set.
        let correlation_sent = correlation_id.is_some() || !matches!(entry, Entry::Client);
        let (command_id, correlation_id, causation_id) = match (entry, &body) {
            (
                Entry::Policy {
                    trigger,
                    correlation_id,
                    command_id,
                },
                _,
            )
            | (
                Entry::Refusal {
                    trigger,
                    correlation_id,
                    command_id,
                },
                _,
            ) => (command_id, correlation_id, Some(trigger)),
            (Entry::Client, Body::Fact { source, .. }) => (
                ids::fact_command_id(&source.external_id),
                correlation_id.unwrap_or_else(|| match process_manager {
                    true => root.clone(),
                    false => ids::fact_correlation(&source.external_id),
                }),
                None,
            ),
            (Entry::Client, Body::Command { .. }) => {
                let id = command_id.unwrap_or_else(|| sampled.command_id.clone());
                let correlation = correlation_id.unwrap_or_else(|| match process_manager {
                    true => root.clone(),
                    false => id.clone(),
                });
                (id, correlation, None)
            }
        };
        Identified {
            cover,
            root,
            body,
            command_id,
            correlation_id,
            correlation_sent,
            causation_id,
            expected_sequence,
            conflict_resolution,
        }
    }
}

/// A [`Checked`] book with its ids assigned: what deduplication looks up
/// and what every event it produces is stamped with
/// ([`Identified::meta`]).
#[derive(Clone, PartialEq, Debug)]
pub struct Identified {
    cover: Cover,
    root: Uuid,
    body: Body,
    command_id: Uuid,
    correlation_id: Uuid,
    /// Whether the correlation was the sender's (or a reaction's
    /// trigger's), not a default filled in here.
    correlation_sent: bool,
    causation_id: Option<EventRef>,
    expected_sequence: Option<u64>,
    conflict_resolution: Option<ConflictResolution>,
}

impl Identified {
    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    pub fn domain(&self) -> &str {
        &self.cover.domain
    }

    pub fn root(&self) -> &Uuid {
        &self.root
    }

    pub fn edition(&self) -> Option<&str> {
        self.cover.edition.as_deref()
    }

    pub fn body(&self) -> &Body {
        &self.body
    }

    pub fn payload(&self) -> &Any {
        self.body.payload()
    }

    /// Always assigned: the deduplication key on the addressed stream.
    pub fn command_id(&self) -> &Uuid {
        &self.command_id
    }

    /// Always assigned.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// The command's own causation (`CommandPage.causation_id`): the
    /// trigger, for a reaction; `None` otherwise. Transport and trace
    /// metadata, and the input its derived command id hashes. It is **not**
    /// stamped on the command's events ([`Identified::meta`]), whose
    /// causation is the command itself (angzarr decision 13.3): the
    /// trigger-to-command edge belongs in tracing and logs.
    pub fn causation_id(&self) -> Option<&EventRef> {
        self.causation_id.as_ref()
    }

    pub fn expected_sequence(&self) -> Option<u64> {
        self.expected_sequence
    }

    pub fn conflict_resolution(&self) -> Option<ConflictResolution> {
        self.conflict_resolution
    }

    /// The metadata every event this command produces carries (angzarr's
    /// `EventMeta`): its causation (this command's id, the deduplication
    /// key), fact source and correlation, the instant it is recorded at and
    /// the rules version it is decided under. No actor: who did it belongs
    /// in a security log. No trigger: a policy command's events name their
    /// command, not the event that set it off; that edge is the command's
    /// own causation (`causation_id`), for tracing.
    pub fn meta(&self, recorded_at: Timestamp, rules_version: &str) -> EventMeta {
        EventMeta {
            causation: Some(event_meta::Causation::CommandId(self.command_id.clone())),
            fact: self.body.source().cloned(),
            correlation_id: Some(self.correlation_id.clone()),
            recorded_at: Some(recorded_at),
            rules_version: rules_version.to_string(),
        }
    }

    /// The book with its ids filled in, handling as sent: what a dead
    /// letter or an audit line carries.
    pub fn to_book(&self) -> CommandBook {
        let body = match &self.body {
            Body::Command { payload } => command_page::Body::Command(angzarr_proto::v1::Command {
                payload: Some(payload.clone()),
            }),
            Body::Fact { payload, .. } => command_page::Body::Fact(angzarr_proto::v1::Fact {
                payload: Some(payload.clone()),
            }),
        };
        let handling = Handling {
            expected_sequence: self.expected_sequence,
            conflict_resolution: self.conflict_resolution.map_or(0, |r| r as i32),
        };
        CommandBook {
            cover: Some(self.cover.clone()),
            page: Some(CommandPage {
                body: Some(body),
                command_id: Some(self.command_id.clone()),
                correlation_id: Some(self.correlation_id.clone()),
                causation_id: self.causation_id.clone(),
                fact_origin: self.body.source().cloned(),
            }),
            handling: (handling != Handling::default()).then_some(handling),
        }
    }

    /// The book handed to a remote domain's coordinator: as
    /// [`Identified::to_book`], its command id fixed here, but a
    /// correlation the sender left unset is unset still, so the remote
    /// coordinator applies its own default (a process manager's root,
    /// decision 14.31; owner ruling, 2026-10-30): remote defaults are the
    /// remote's.
    pub fn to_remote_book(&self) -> CommandBook {
        let mut book = self.to_book();
        if !self.correlation_sent {
            if let Some(page) = book.page.as_mut() {
                page.correlation_id = None;
            }
        }
        book
    }
}

#[cfg(test)]
#[path = "check.test.rs"]
mod tests;

//! What angzarr-embedded-macros' expansion names: the runtime under a marked-up
//! aggregate or process manager (`#[aggregate]`, `#[process_manager]`,
//! `#[handles_cmd]`, `#[handles_fact]`, `#[on_refused]`, `#[applies]`),
//! policy (`#[policy]`, `#[on_event]`) and projector (`#[projector]`,
//! `#[projects]`); see angzarr-embedded-macros' crate docs. A hand-written
//! [`Domain`] needs none of it; a hand-written component that wants what
//! the markup does uses the same functions the expansion calls.
//!
//! - [`Context`]: a domain handler's third argument, what it may read
//!   besides itself and its input: one context, an aggregate's and a
//!   process manager's alike.
//! - [`DomainMessage`]: a domain type (angzarr-codegen's domain layer) and
//!   the prost message it travels as, so the router decodes by type URL
//!   into domain types and never hands a handler a wire type.
//! - [`Emits`] and [`Emit`]: what a command handler's `Ok` may be. A shape
//!   (`E`, a tuple, `Option<E>`, `Vec<E>`) is events the router applies, in
//!   order; [`Emit::then`] adds a continuation step the router runs on the
//!   instance after them, within the same decision.
//! - [`Record`] and [`Records`]: what a FACT handler returns: as [`Emit`],
//!   but its continuation steps return no `Result`.
//! - [`Rejection`], [`Refusing`], [`Refuses`] and [`TypedRefusal`]: a
//!   command's refusal, a message only or one of its declared typed
//!   refusal objects.
//! - [`Failed`], [`BusinessStatus`], [`RetryableStatus`] and [`Fails`]:
//!   what a command handler or a continuation step fails with, a business
//!   refusal (under a status of the business set), an immediate return
//!   (ABORTED), a fatal bug, or a retryable bug (under the status it
//!   picks).
//! - [`CommandRefused`] and [`Refusal`]: a refusal fact as its source's
//!   `#[on_refused]` handler reads it, and a refusal as a translating
//!   policy's callback reads it.
//! - [`ProcessManager`]: what sets a process manager, a domain, apart: its
//!   starting inputs.
//! - [`Routed`]: a marked-up domain's handlers, registered on
//!   angzarr-router's table.
//! - [`Policy`], [`Commands`] and [`PolicyContext`]: a policy, its output
//!   and its handler's context. [`Decides`] and [`Ingests`] say what a
//!   domain takes, for the checks on a policy.
//! - [`AsyncProjector`] and [`ProjectionContext`]: a projector.
//! - [`STEP_LIMIT`] and [`assert_step_idempotent`]: the cap on a decision's
//!   continuation steps, and the test helper for a step.
//! - [`TypeName`]: a message's package and name, as the generated
//!   constants list them.
//! - [`NoSnapshot`]: the snapshot form of a domain that has none.
//! - The functions the expansion calls, each stable and public, so
//!   anything the markup writes can be written by hand
//!   (`src/hand_written.test.rs` registers an aggregate and a policy
//!   with no macro): packing and decoding ([`pack_message`],
//!   [`type_url_of`], [`decode_message`], [`command_refused_type_url`],
//!   [`decode_command_refused`], [`refused_command_type_url`]); running a
//!   domain's handler as one decision ([`run_command`], [`run_fact`],
//!   [`run_on_refused`]); a policy's answers ([`policy_answer`],
//!   [`pass_through`], [`decode_refusal`], [`translated_fact`]); a
//!   projector's run ([`projecting`]); and a state's descriptor
//!   ([`state_descriptor`]).
//! - [`v1`]: angzarr's refusal messages, until angzarr-embedded's angzarr-proto has
//!   them.
//!
//! Every component here assumes nothing about distribution: the same
//! marked-up aggregate, process manager, policy or projector runs in
//! angzarr-embedded's process or as its own service, with no change to its code.
//!
//! angzarr-embedded's `src/decorated.test.rs` runs all of it.

use std::fmt;

use angzarr_proto::v1::{Cover, EventBook, EventRef, Uuid};
use prost_types::{Any, Timestamp};

use super::domain::Domain;
use super::error::PayloadError;
use super::payload::Payload as _;
use super::wire::WireError;

/// The error a generated `State::from_snapshot` returns, re-exported so
/// the expansion needs no dependency of its user's.
pub use angzarr_diagnostic::Report;

/// angzarr-router's context of the input a handler decides, and its
/// conflict under AGGREGATE_HANDLES: a [`Context`]'s `command`.
pub use angzarr_router::domain::{CommandContext, Conflict};

/// The statuses a business refusal may go out under, angzarr-router's
/// business set: a [`Failed::Rejected`]'s status. Any other status is a
/// fault.
pub use angzarr_router::error::BusinessStatus;

/// The statuses a handler's retryable bug may go out under,
/// angzarr-router's: a [`Failed::RetryableBug`]'s status. They are not
/// interchangeable, and the kernel's capped retry and any caller of its
/// own honour the difference:
///
/// - `Unavailable` (UNAVAILABLE): resend; the dependency is down for now.
/// - `DeadlineExceeded` (DEADLINE_EXCEEDED): resend only if the call is
///   idempotent, since the work may have happened. A command is, by its
///   `command_id` (the kernel's deduplication).
/// - `ResourceExhausted` (RESOURCE_EXHAUSTED): wait longer before
///   resending; a quota or a capacity limit was hit.
pub use angzarr_router::error::RetryableStatus;

/// What a handler, or a continuation step, reads besides the instance and
/// its input, all read-only: the ONE handler context of every domain, an
/// aggregate's and a process manager's alike.
///
/// - [`Context::command`]: angzarr-router's whole
///   [`CommandContext`](angzarr_router::domain::CommandContext), as the
///   router hands it to the handler: `cover` (the stream's domain and
///   root), `next_sequence`, `had_prior_events`, `expected_sequence`,
///   `conflict_resolution`, `conflict` (under AGGREGATE_HANDLES, the window
///   the command did not see and the nonce the handler hands back through
///   `resolve_conflicts`), and the input's identity: `command_id` (a
///   client's, the one the application minted, a policy's derived one, or,
///   for a fact, the one derived from its origin; a refusal fact's is the
///   refused command's), `correlation_id` and, for a fact, `fact_origin`.
///   Nothing the router knows of the input is withheld, and nothing it
///   knows is kept twice.
/// - [`Context::recorded_at`]: the instant the input was accepted and its
///   events will be recorded at, the router's `recorded_at`
///   (`ContextualCommand.recorded_at`, decision 14.37), sampled by the
///   application and stamped by the kernel, so the handler stays pure:
///   there is no wall clock.
#[derive(Clone, Debug, Default)]
pub struct Context {
    /// angzarr-router's context of the input this handler decides.
    pub command: CommandContext,
}

impl Context {
    /// A context for a handler's own unit tests, recorded at `recorded_at`
    /// with the router's default context otherwise
    /// ([`Context::with_command`] sets it); the kernel builds the real one
    /// from what angzarr-router hands the handler.
    pub fn new(recorded_at: Timestamp) -> Context {
        Context {
            command: CommandContext {
                recorded_at: Some(recorded_at),
                ..CommandContext::default()
            },
        }
    }

    /// This context, with `command` as the router's context of the input,
    /// its instant included.
    pub fn with_command(self, command: CommandContext) -> Context {
        Context { command }
    }

    /// When the events will be recorded (non-decreasing within the stream):
    /// the router's `recorded_at`.
    pub fn recorded_at(&self) -> Timestamp {
        self.command.recorded_at.unwrap_or_default()
    }
}

/// A domain type and the wire message it travels as. angzarr-codegen's
/// domain layer emits the impl beside each domain type; a prost message
/// does not implement it, so a handler or applier taking a wire type does
/// not compile.
pub trait DomainMessage: Clone + Send + Sync + Sized + 'static {
    /// The prost message: its [`prost::Name`] is the type URL the router
    /// routes by.
    type Wire: prost::Message + prost::Name + Default;

    /// This value on the wire.
    fn to_wire(&self) -> Self::Wire;

    /// The domain value a decoded message holds, or why it holds none.
    fn from_wire(wire: Self::Wire) -> Result<Self, WireError>;
}

/// A continuation step: a pure `&self` method of the aggregate (or a
/// closure over one), run on the instance with every event so far applied.
/// It fails as a handler does ([`Fails`]): a refusal with the domain's own
/// rejection or a [`Refuses`] choice, as is or as a [`Failed`], or a bug.
type Step<A> = Box<dyn FnOnce(&A, &Context) -> Result<Emit<A>, Failed<<A as Domain>::Rejection>>>;

/// Events for the router to apply, in order, and optionally one
/// continuation step to run on the instance after them.
///
/// ```text
/// fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> {
///     Ok(Emit::of(Deposited { .. }).then(Self::settle))
/// }
///
/// fn settle(&self, ctx: &Context) -> Result<Option<Rewarded>, Refused> { .. }
/// ```
///
/// The router applies the events, calls the step on the updated instance,
/// applies what it returns, and repeats while there is a continuation. All
/// of it is **one decision**: a rejection from the handler or from any step
/// refuses it all, a bug from any of them fails it all as the handler's
/// bug would, and what is committed is every step's events, in order, in
/// one commit. More than [`STEP_LIMIT`] steps is a bug, and the kernel's
/// [`KernelError::TooManySteps`](super::KernelError::TooManySteps).
///
/// A continuation is the only way a later event is decided from the state
/// after an earlier one. Applying an event never sets off processing: an
/// applier only assigns state.
pub struct Emit<A: Domain> {
    events: Vec<A::Event>,
    next: Option<Step<A>>,
}

impl<A: Domain> Emit<A> {
    /// These events, and no continuation.
    pub fn of(events: impl Emits<A>) -> Emit<A> {
        events.into_emit()
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return an `Emit` with a continuation.
    pub fn then<S, O, E>(self, step: S) -> Emit<A>
    where
        S: FnOnce(&A, &Context) -> Result<O, E> + 'static,
        O: Emits<A>,
        E: Fails<A>,
    {
        self.push(Box::new(move |instance, ctx| {
            step(instance, ctx)
                .map(Emits::into_emit)
                .map_err(Fails::into_failed)
        }))
    }

    /// `step` after this emit's continuation, if it has one, else as it.
    fn push(self, step: Step<A>) -> Emit<A> {
        let next: Step<A> = match self.next {
            None => step,
            Some(first) => {
                Box::new(move |instance, ctx| first(instance, ctx).map(|emit| emit.push(step)))
            }
        };
        Emit {
            events: self.events,
            next: Some(next),
        }
    }
}

impl<A: Domain> fmt::Debug for Emit<A>
where
    A::Event: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Emit")
            .field("events", &self.events)
            .field("then", &self.next.is_some())
            .finish()
    }
}

/// What a handler's or a step's `Ok` may be, for aggregate `A`:
///
/// - a domain event `T`: one event;
/// - a tuple `(T1, T2)`, `(T1, T2, T3)`, `(T1, T2, T3, T4)`: a fixed set,
///   in order;
/// - `Option<T>`: one or none;
/// - `Vec<T>`: any number of one type;
/// - an [`Emit`]: events and a continuation.
///
/// A variable list of different types is not a shape (deferred).
pub trait Emits<A: Domain> {
    /// The events and the continuation, if any.
    fn into_emit(self) -> Emit<A>;
}

impl<A: Domain> Emits<A> for Emit<A> {
    fn into_emit(self) -> Emit<A> {
        self
    }
}

/// These events, with no continuation.
fn emitted<A: Domain>(events: Vec<A::Event>) -> Emit<A> {
    Emit { events, next: None }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for T {
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.into()])
    }
}

impl<A, T1, T2> Emits<A> for (T1, T2)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.0.into(), self.1.into()])
    }
}

impl<A, T1, T2, T3> Emits<A> for (T1, T2, T3)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
    T3: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.0.into(), self.1.into(), self.2.into()])
    }
}

impl<A, T1, T2, T3, T4> Emits<A> for (T1, T2, T3, T4)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
    T3: DomainMessage + Into<A::Event>,
    T4: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
        ])
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Option<T> {
    fn into_emit(self) -> Emit<A> {
        emitted(self.into_iter().map(Into::into).collect())
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Vec<T> {
    fn into_emit(self) -> Emit<A> {
        emitted(self.into_iter().map(Into::into).collect())
    }
}

/// What a FACT handler (`#[handles_fact]`) returns: events for the router
/// to apply, in order, and optionally one continuation step, as [`Emit`],
/// but nothing in it can refuse. A fact already happened ("if it can't be
/// refused, it's a fact"), so its handler returns no `Result`, and neither
/// does any step: a step is `fn(&self, ctx: &Context) -> R`, `R` a shape
/// ([`Records`]) or another `Record`. Same router semantics as [`Emit`]:
/// one decision, every step's events committed together, [`STEP_LIMIT`].
///
/// ```text
/// fn payout(&self, fact: Payout, ctx: &Context) -> Record<Self> {
///     Record::of(PaidOut { .. }).then(Self::emptied)
/// }
///
/// fn emptied(&self, ctx: &Context) -> Option<Emptied> { .. }
/// ```
///
/// A fact handler's continuation steps are infallible: a separate type,
/// not `Emit`, so a fallible step in a fact's chain does not compile.
pub struct Record<A: Domain> {
    events: Vec<A::Event>,
    next: Option<RecordStep<A>>,
}

/// A fact's continuation step: infallible.
type RecordStep<A> = Box<dyn FnOnce(&A, &Context) -> Record<A>>;

impl<A: Domain> Record<A> {
    /// These events, and no continuation.
    pub fn of(events: impl Records<A>) -> Record<A> {
        events.into_record()
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return a `Record` with a continuation.
    pub fn then<S, O>(self, step: S) -> Record<A>
    where
        S: FnOnce(&A, &Context) -> O + 'static,
        O: Records<A>,
    {
        self.push(Box::new(move |instance, ctx| {
            step(instance, ctx).into_record()
        }))
    }

    /// `step` after this record's continuation, if it has one, else as it.
    fn push(self, step: RecordStep<A>) -> Record<A> {
        let next: RecordStep<A> = match self.next {
            None => step,
            Some(first) => Box::new(move |instance, ctx| first(instance, ctx).push(step)),
        };
        Record {
            events: self.events,
            next: Some(next),
        }
    }
}

impl<A: Domain> fmt::Debug for Record<A>
where
    A::Event: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("events", &self.events)
            .field("then", &self.next.is_some())
            .finish()
    }
}

/// What a fact handler, or a fact's continuation step, may return: the
/// [`Emits`] shapes (an event, a tuple of up to four, `Option`, `Vec`) or
/// a [`Record`]; never an [`Emit`], whose steps may refuse.
pub trait Records<A: Domain> {
    /// The events and the continuation, if any.
    fn into_record(self) -> Record<A>;
}

impl<A: Domain> Records<A> for Record<A> {
    fn into_record(self) -> Record<A> {
        self
    }
}

macro_rules! records_shape {
    ($([$($t:ident),+] $shape:ty;)*) => {$(
        impl<A: Domain, $($t: DomainMessage + Into<A::Event>),+> Records<A> for $shape {
            fn into_record(self) -> Record<A> {
                let Emit { events, .. } = Emits::<A>::into_emit(self);
                Record { events, next: None }
            }
        }
    )*};
}

records_shape! {
    [T] T;
    [T1, T2] (T1, T2);
    [T1, T2, T3] (T1, T2, T3);
    [T1, T2, T3, T4] (T1, T2, T3, T4);
    [T] Option<T>;
    [T] Vec<T>;
}

/// A process manager written in the markup (`#[process_manager]`). A
/// process manager IS A DOMAIN: its inputs are commands (which policies send
/// it, and which it may refuse) and facts (timeouts, webhooks, refusal
/// facts, which it never refuses); its only output is its own events. It
/// subscribes to nothing: the policy for (another domain, the PM's domain)
/// turns that domain's events into commands to it. The macro gives it the
/// same `Domain` and `State` impls an `#[aggregate]` gets; this trait adds
/// the two things that set a PM apart. Registered with
/// [`Registry::process_manager`](super::Registry::process_manager).
///
/// - **Its root is the one its input names.** A command or fact addressed
///   to it names the instance in its cover, whatever its correlation (one
///   instance may take inputs of several correlations); a `correlation_id`
///   left unset is given the root.
/// - **The start rule**, the kernel's. Only an input type it declares
///   as starting (`#[handles_cmd(C, starts)]`, `#[handles_fact(F, starts)]`)
///   may start an instance. Any other input for a stream with no instance is
///   not delivered, decided before the handler by the type after
///   upcasting: a FACT is skipped silently (applied, no events, nothing
///   dead-lettered), and a COMMAND is refused (`COORDINATOR_REASON_PM_NO_INSTANCE`, forwarded
///   as `angzarr-embedded-pm-no-instance`, a business refusal, so a
///   policy's refused command yields its refusal fact like any other),
///   recording nothing.
///
/// A process manager whose `STARTS` is empty can never start an instance;
/// angzarr-embedded does not refuse one (no compile error, no check): the warning is
/// angzarr-cli's.
///
/// The kernel registers it on the router's process-manager table, and
/// applies the start rule by `STARTS`: the descriptor's starting types,
/// which the markup generates (decision 14.42).
pub trait ProcessManager: Domain<State = Self> {
    /// The input types (commands and facts) that may start an instance,
    /// in marker order: angzarr's `ComponentDescriptor.starting_types`.
    const STARTS: &'static [TypeName];
}

/// That `A` decides command `C`: `#[handles_cmd(C)]` on its impl, or, for
/// a remote domain's command set ([`CommandTarget`]), written beside it.
/// What a policy's [`Commands`] is checked against. Emitted by the macro,
/// one per command.
pub trait Decides<C: DomainMessage>: CommandTarget {}

/// That `A` ingests fact `F`: `#[handles_fact(F)]` on its impl, or, for
/// a child-hosted domain's event set ([`EventSource`]), written beside it.
/// What a translating policy's callback is checked against: it must return
/// a fact its source ingests. Emitted by the macro, one per fact.
pub trait Ingests<F: DomainMessage>: EventSource {}

// ---------------------------------------------------------------- refusals

/// The typed refusal objects `A`'s handler for command `C` may refuse
/// with: `#[handles_cmd(C, refuses(T1, T2, ..))]`, the markup's
/// `CommandOptions.refuses`. The macro emits it for each such command,
/// with [`Refusing::Refuses`] a choice of exactly those types, each
/// `From` its type (and the choice `Into` the domain's [`Rejection`]): a
/// handler returning `Err(T.into())` for a `T` not listed does not
/// compile.
pub trait Refusing<C: DomainMessage>: Domain {
    /// The choice of the listed refusal objects.
    type Refuses: Into<TypedRefusal>;
    /// The listed types, in order.
    const REFUSES: &'static [TypeName];
}

/// What a handler with `refuses(..)` returns as its `Err`:
/// `Result<Held, Refuses<Self, Hold>>`.
pub type Refuses<A, C> = <A as Refusing<C>>::Refuses;

/// A typed refusal object, packed: one of a command's `refuses` types (a
/// [`DomainMessage`] of the command's own domain). Keyed, as on the wire,
/// ROUTER_REASON_COMMAND_REFUSED with the object's full type name as `refusal_type`
/// (decision 14.37): the object is the refusal's data.
#[derive(Clone, PartialEq, Debug)]
pub struct TypedRefusal {
    object: Any,
}

/// The code a typed refusal is keyed under: angzarr's ROUTER_REASON_COMMAND_REFUSED.
const COMMAND_REFUSED: &str = "angzarr-embedded-command-refused";

impl TypedRefusal {
    /// `object`, packed.
    pub fn of<T: DomainMessage + angzarr_diagnostic::Diagnostic>(object: &T) -> TypedRefusal {
        TypedRefusal {
            object: pack_message(object),
        }
    }

    /// The object's full type name: its `refusal_type`.
    fn refusal_type(&self) -> &str {
        super::payload::type_name(&self.object.type_url)
    }

    /// The object, as `T`, if it is one.
    pub fn object<T: DomainMessage>(&self) -> Option<T> {
        decode_message(&self.object).ok().flatten()
    }
}

/// A marked-up domain's `Domain::Rejection`: a handler's refusal, as the
/// caller (or a refused policy) receives it.
///
/// - [`Rejection::Message`]: a command handler without `refuses` refused
///   with the domain's own rejection `R` (inferred from those handlers, or
///   given by `rejection = ..`), a message only, as is.
/// - [`Rejection::Typed`]: a handler with `refuses` refused with one of
///   its listed objects.
///
/// Either way the refusing domain records nothing.
#[derive(Clone, PartialEq, Debug)]
pub enum Rejection<R> {
    Message(R),
    Typed(TypedRefusal),
}

/// A message-only refusal: a handler's or a step's `Err` of the domain's
/// own rejection.
impl<R> From<R> for Rejection<R> {
    fn from(rejection: R) -> Self {
        Rejection::Message(rejection)
    }
}

impl<R: angzarr_diagnostic::Diagnostic> Rejection<R> {
    /// As a refused policy receives it, the oneof: the typed object
    /// packed, or a message-only refusal's code and args.
    pub fn response(&self) -> v1::RefusalResponse {
        v1::RefusalResponse {
            reason: Some(match self {
                Rejection::Typed(typed) => {
                    v1::refusal_response::Reason::Refusal(typed.object.clone())
                }
                Rejection::Message(_) => {
                    v1::refusal_response::Reason::Diagnostic(super::route::diagnostic(self))
                }
            }),
        }
    }
}

impl<R> Rejection<R> {
    /// The typed refusal object, packed: `None` unless [`Rejection::Typed`].
    pub fn refusal(&self) -> Option<&Any> {
        match self {
            Rejection::Typed(typed) => Some(&typed.object),
            Rejection::Message(_) => None,
        }
    }
}

impl<R: angzarr_diagnostic::Diagnostic> fmt::Display for Rejection<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl<R: angzarr_diagnostic::Diagnostic> angzarr_diagnostic::Diagnostic for Rejection<R> {
    fn code(&self) -> &str {
        match self {
            Rejection::Message(rejection) => rejection.code(),
            Rejection::Typed(_) => COMMAND_REFUSED,
        }
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        match self {
            Rejection::Message(rejection) => rejection.args(),
            Rejection::Typed(typed) => vec![("refusal_type", typed.refusal_type().into())],
        }
    }
}

/// What a command handler, or a continuation step, fails with: one of
/// decision 14.37's four classes. `X` is what it refuses with: the
/// domain's own rejection, or, for a handler with `refuses(..)`, its
/// [`Refuses`] choice.
///
/// - [`Failed::Rejected`] (business, fatal): the domain said no. It goes
///   out as a business refusal under `status` (FAILED_PRECONDITION unless
///   the handler says otherwise, [`Failed::with_status`]), the refusal's
///   code and args beside, and reaches the issuing policy's refusal path.
/// - [`Failed::Immediate`] (business, immediate): "re-read and
///   re-decide". It goes out as ABORTED under the report's own code and
///   args (angzarr-router's `CodedError::immediate`): back to an outside
///   caller at once, never retried or dead-lettered; a policy's step,
///   whose caller is the kernel, is reloaded and run again under the
///   capped backoff, then dead-lettered. Never a policy's refusal path.
/// - [`Failed::Bug`] (bug, fatal): a defect, never a business answer: the
///   report of the bug value (a policy's `Err` is the same), its code and
///   args. It goes out as INTERNAL ROUTER_REASON_UNHANDLED_HANDLER_ERROR, the code as its
///   `cause` and each arg as `cause.<arg>`, and the input is dead-lettered
///   at once.
/// - [`Failed::RetryableBug`] (bug, retryable): a defect outside the
///   handler that may clear (a dependency that is down), under the
///   [`RetryableStatus`] the handler picks. It goes out under that status
///   as ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its cause as a fatal bug's
///   (angzarr-router's `CodedError::retryable_bug`); the kernel retries
///   it with capped backoff, then dead-letters it with its `retry_count`.
///   Never a policy's refusal path.
///
/// A handler or step that only refuses keeps returning `X` as is: it
/// converts into a `Failed` by `From`, so `?` on a `Result<_, X>` works
/// inside one returning `Failed<X>`.
///
/// ```text
/// fn gauge(&self, cmd: Gauge, ctx: &Context) -> Result<Gauged, Failed<Refused>> {
///     if broken { return Err(Failed::bug(&MeterBroke { seat })); }
///     if offline { return Err(Failed::retryable_bug(RetryableStatus::Unavailable, &MeterOffline { seat })); }
///     if moved { return Err(Failed::immediate(&SeatMoved { seat })); }
///     if cmd.reading < 0 {
///         return Err(Failed::from(Refused::Negative).with_status(BusinessStatus::InvalidArgument));
///     }
///     within(cmd.reading)?; // a Result<_, Refused>: FAILED_PRECONDITION
///     ..
/// }
///
/// fn hold(&self, cmd: Hold, ctx: &Context) -> Result<Held, Failed<Refuses<Self, Hold>>> {
///     if broken { return Err(Failed::bug(&WalletBroke {})); }
///     Err(Failed::rejected(InsufficientFunds { short }))
/// }
/// ```
#[derive(Clone, PartialEq, Debug)]
pub enum Failed<X> {
    Rejected {
        rejection: X,
        status: BusinessStatus,
    },
    Immediate(Report),
    Bug(Report),
    RetryableBug {
        status: RetryableStatus,
        report: Report,
    },
}

impl<X> Failed<X> {
    /// A refusal with `rejection`, FAILED_PRECONDITION.
    pub fn rejected(rejection: impl Into<X>) -> Failed<X> {
        Failed::Rejected {
            rejection: rejection.into(),
            status: BusinessStatus::FailedPrecondition,
        }
    }

    /// A bug, keyed by `bug`'s code, with its args.
    pub fn bug(bug: &dyn angzarr_diagnostic::Diagnostic) -> Failed<X> {
        Failed::Bug(Report::of(bug))
    }

    /// A retryable bug under `status`, keyed by `bug`'s code, with its
    /// args.
    pub fn retryable_bug(
        status: RetryableStatus,
        bug: &dyn angzarr_diagnostic::Diagnostic,
    ) -> Failed<X> {
        Failed::RetryableBug {
            status,
            report: Report::of(bug),
        }
    }

    /// An immediate return (ABORTED), keyed by `why`'s code, with its args.
    pub fn immediate(why: &dyn angzarr_diagnostic::Diagnostic) -> Failed<X> {
        Failed::Immediate(Report::of(why))
    }

    /// This refusal under `status`; any other failure is unchanged.
    pub fn with_status(self, status: BusinessStatus) -> Failed<X> {
        match self {
            Failed::Rejected { rejection, .. } => Failed::Rejected { rejection, status },
            other => other,
        }
    }

    /// This failure, its refusal converted into `Y`: a typed refusal
    /// object's into its command's [`Refuses`] choice, say.
    pub fn widen<Y>(self) -> Failed<Y>
    where
        X: Into<Y>,
    {
        match self {
            Failed::Rejected { rejection, status } => Failed::Rejected {
                rejection: rejection.into(),
                status,
            },
            Failed::Immediate(report) => Failed::Immediate(report),
            Failed::Bug(report) => Failed::Bug(report),
            Failed::RetryableBug { status, report } => Failed::RetryableBug { status, report },
        }
    }
}

/// A refusal, FAILED_PRECONDITION: a handler's `Err` (or `?`) of what it
/// refuses with.
impl<X> From<X> for Failed<X> {
    fn from(rejection: X) -> Self {
        Failed::rejected(rejection)
    }
}

/// The refusal's code and args, or the report's (decision 14.37): never
/// English.
impl<X: angzarr_diagnostic::Diagnostic> fmt::Display for Failed<X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failed::Rejected { rejection, .. } => angzarr_diagnostic::display(rejection, f),
            Failed::Immediate(report)
            | Failed::Bug(report)
            | Failed::RetryableBug { report, .. } => angzarr_diagnostic::display(report, f),
        }
    }
}

/// What a command handler's or a continuation step's `Err` may be, for
/// domain `A`: a [`Failed`] of anything `Into` the domain's rejection, or,
/// as the macro emits for each domain, its own rejection or a
/// [`Refuses`] choice as is.
pub trait Fails<A: Domain> {
    /// This failure, its refusal as the domain's rejection.
    fn into_failed(self) -> Failed<A::Rejection>;
}

impl<A: Domain, X: Into<A::Rejection>> Fails<A> for Failed<X> {
    fn into_failed(self) -> Failed<A::Rejection> {
        self.widen()
    }
}

/// The pass-through refusal fact, as the policy's SOURCE domain's handler
/// for it reads it: `#[on_refused(C)]`, a fact handler,
/// `fn(&self, refused: CommandRefused<C>, ctx: &Context) ->
/// Result<Record<Self>, E>` (or a shape), `ctx` the router's context of
/// the refusal fact. It cannot refuse; its `Err`, any `Diagnostic`, is a
/// bug, its policy's failure
/// ([`StepError::PolicyFailed`](super::StepError::PolicyFailed)). The macro dispatches angzarr's
/// `CommandRefused` fact by the refused command's type to the handler for
/// it: which refusals a domain handles is inferred from its handlers, so a
/// `CommandRefused` for a command it has no handler for is caught at run
/// time (`angzarr-embedded-no-refusal-handler`, dead-lettered).
#[derive(Clone, PartialEq, Debug)]
pub struct CommandRefused<C> {
    command: C,
    target: Cover,
    refusal: Option<Any>,
    trigger: EventRef,
}

impl<C> CommandRefused<C> {
    /// A refusal fact for a handler's own unit tests; the router builds the
    /// real one from angzarr's `CommandRefused`.
    pub fn new(command: C, target: Cover, refusal: Option<Any>, trigger: EventRef) -> Self {
        CommandRefused {
            command,
            target,
            refusal,
            trigger,
        }
    }

    /// The refused command, as sent.
    pub fn command(&self) -> &C {
        &self.command
    }

    /// The stream that refused it.
    pub fn target(&self) -> &Cover {
        &self.target
    }

    /// The typed refusal object, as `T`, if the target refused with one
    /// and it is a `T`.
    pub fn refusal<T: DomainMessage>(&self) -> Option<T> {
        decode_message(self.refusal.as_ref()?).ok().flatten()
    }

    /// The policy's triggering event: a page of this stream.
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }
}

/// What a policy's translating callback reads (`#[on_refused(C)]` on the
/// policy's impl): angzarr's `Refusal { event, command,
/// response }`, the command decoded, beside the refused command's
/// [`CommandContext`]. The callback returns `Result` of a fact of the
/// policy's source domain, so the source never refers to the target's
/// types; its `Err` is a bug, dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed).
#[derive(Clone, PartialEq, Debug)]
pub struct Refusal<C> {
    event: EventBook,
    command: C,
    response: v1::RefusalResponse,
}

impl<C> Refusal<C> {
    /// A refusal for a callback's own unit tests; the policy's generated
    /// `on_refused` builds the real one.
    pub fn new(event: EventBook, command: C, response: v1::RefusalResponse) -> Self {
        Refusal {
            event,
            command,
            response,
        }
    }

    /// The triggering event: a book of the source stream holding the one
    /// event the policy reacted to.
    pub fn event(&self) -> &EventBook {
        &self.event
    }

    /// The refused command, as sent.
    pub fn command(&self) -> &C {
        &self.command
    }

    /// The typed refusal object, as `T`, if the target refused with one
    /// and it is a `T`.
    pub fn refusal<T: DomainMessage>(&self) -> Option<T> {
        match self.response.reason.as_ref()? {
            v1::refusal_response::Reason::Refusal(object) => decode_message(object).ok().flatten(),
            v1::refusal_response::Reason::Diagnostic(_) => None,
        }
    }

    /// A refusal without a typed object: its code and args (decision
    /// 14.37), never English; `None` for a typed one.
    pub fn diagnostic(&self) -> Option<&angzarr_proto::v1::Diagnostic> {
        match self.response.reason.as_ref()? {
            v1::refusal_response::Reason::Diagnostic(diagnostic) => Some(diagnostic),
            v1::refusal_response::Reason::Refusal(_) => None,
        }
    }
}

// ---------------------------------------------------------------- policies

/// A policy: the kernel's one [`Policy`](super::Policy) trait, which
/// `#[policy]` implements (answering refusals) and a hand-written policy
/// implements as it is. Policies should not fail for business reasons:
/// they react to events the application's own domains created, so an `Err`
/// is a bug, dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed).
pub use super::policy::{CommandTarget, EventSource, Policy};

/// What a policy's `#[on_event(E)]` handler returns as its `Ok` (its `Err`
/// is any [`Diagnostic`](angzarr_diagnostic::Diagnostic): a bug,
/// dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed)): commands for its target
/// `T` ([`CommandTarget`]: a domain here, or a remote one's command set),
/// each with the root of the stream it is for. For a process manager
/// that is the trigger's correlation ([`PolicyContext::correlation_id`]);
/// any other root is a policy bug. Each command's type must be one
/// `T` decides ([`Decides`]).
pub struct Commands<T: CommandTarget> {
    orders: Vec<(Uuid, Any)>,
    target: std::marker::PhantomData<fn() -> T>,
}

impl<T: CommandTarget> Commands<T> {
    /// No commands.
    pub fn none() -> Commands<T> {
        Commands {
            orders: Vec::new(),
            target: std::marker::PhantomData,
        }
    }

    /// These commands and `command`, for the stream of `T` at `root`.
    pub fn to<C>(self, root: Uuid, command: C) -> Commands<T>
    where
        C: DomainMessage,
        T: Decides<C>,
    {
        let mut orders = self.orders;
        orders.push((root, pack_message(&command)));
        Commands {
            orders,
            target: self.target,
        }
    }

    /// Each command, packed, with the root of its stream, in order.
    pub fn orders(&self) -> &[(Uuid, Any)] {
        &self.orders
    }

    /// As [`Commands::orders`], owned.
    pub(crate) fn into_orders(self) -> Vec<(Uuid, Any)> {
        self.orders
    }
}

impl<T: Domain> Commands<T> {
    /// These commands and `command`, one of `T`'s, for the stream of `T` at
    /// `root`: a hand-written policy's way to send one of a domain's
    /// command payload.
    pub fn send(self, root: Uuid, command: &<T as Domain>::Command) -> Commands<T> {
        let mut orders = self.orders;
        orders.push((root, command.to_any()));
        Commands {
            orders,
            target: self.target,
        }
    }
}

/// What a policy's handler reads besides the event: the triggering event's
/// `EventRef` (its stream and sequence), the root of that stream, its
/// correlation and when it was recorded.
#[derive(Clone, PartialEq, Debug)]
pub struct PolicyContext {
    trigger: EventRef,
    source_root: Uuid,
    correlation_id: Uuid,
    recorded_at: Timestamp,
}

impl PolicyContext {
    /// A context for a policy's own unit tests; the router builds the real
    /// one from the committed event.
    pub fn new(trigger: EventRef, correlation_id: Uuid, recorded_at: Timestamp) -> Self {
        let source_root = trigger
            .stream
            .as_ref()
            .and_then(|cover| cover.root.clone())
            .unwrap_or_default();
        PolicyContext {
            trigger,
            source_root,
            correlation_id,
            recorded_at,
        }
    }

    /// The event reacted to.
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }

    /// The root of the stream the event was recorded on, the source's: the
    /// trigger's `stream.root`. A policy whose target stream is the source
    /// stream's counterpart (the same root in another domain) sends its
    /// command there. The kernel's trigger always names its stream; a
    /// context built by hand whose trigger names none has the empty root.
    pub fn source_root(&self) -> &Uuid {
        &self.source_root
    }

    /// The event's correlation: the root of a process manager's instance.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// When the event was recorded.
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }
}

/// angzarr's refusal messages (`io.angzarr.v1`'s `CommandRefused`,
/// `RefusalResponse` and `Refusal`, decision 14.31), as angzarr-router
/// carries them.
pub mod v1 {
    pub use angzarr_router::pb::{refusal_response, CommandRefused, Refusal, RefusalResponse};
}

/// A projector written in the markup (`#[projector]`): one or more source
/// domains, one instance per process, run after commit and awaited before
/// the call returns. A failure is the caller's error,
/// [`Failure::Projection`](super::Failure::Projection), carrying what was
/// committed: the events stay committed. Projectors run on the async
/// command path, the only one (async everywhere), so there is no guard
/// against blocking on a synchronous one. Registered with
/// [`Registry::projector`](super::Registry::projector). Several
/// projectors on one domain each receive every committed event of it they
/// mark; one's failure does not stop the others. The kernel's own
/// interface to a projector is async too: a hand-written one implements
/// this trait, or angzarr-router's `ProjectorHandler` for
/// [`Registry::projector_handler`](super::Registry::projector_handler).
pub trait AsyncProjector: Send + 'static {
    /// Named in the error a failure raises.
    const NAME: &'static str;
    /// The domains whose events it projects, by name, in marker order:
    /// at least one. Its one instance receives the committed events of
    /// each, every stream's in its order;
    /// [`ProjectionContext::stream`] says which stream an event is from.
    const SOURCES: &'static [&'static str];
    /// The projected events, in marker order.
    const PROJECTS: &'static [TypeName];

    /// Project `event` (an event of one of its sources, packed), from its
    /// `#[projects]` handler for the event's type; `None` for a type it
    /// does not mark. Generated.
    fn project<'a>(
        &'a mut self,
        event: &'a Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>>;
}

/// A projector handler's run: awaited by the kernel after the commit.
pub type Projecting<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Report>> + Send + 'a>>;

/// What a projector's handler reads besides the event: the stream it is
/// from, its sequence there and when it was recorded.
#[derive(Clone, PartialEq, Debug)]
pub struct ProjectionContext {
    stream: Cover,
    sequence: u64,
    recorded_at: Timestamp,
}

impl ProjectionContext {
    /// A context for a projector's own unit tests; the kernel builds the
    /// real one from the committed event.
    pub fn new(stream: Cover, sequence: u64, recorded_at: Timestamp) -> Self {
        ProjectionContext {
            stream,
            sequence,
            recorded_at,
        }
    }

    /// The source stream.
    pub fn stream(&self) -> &Cover {
        &self.stream
    }

    /// The event's sequence in its stream.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// When the event was recorded.
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }
}

/// The most continuation steps one decision may run, after its handler.
/// A chain that would run more is a bug (a step that always continues),
/// refused as [`KernelError::TooManySteps`](super::KernelError::TooManySteps)
/// with nothing committed.
pub const STEP_LIMIT: usize = 16;

/// The test helper for a continuation step: runs `step` on `instance`,
/// applies what it emits (following its continuations, as the router
/// does, up to [`STEP_LIMIT`]), then runs `step` again on the result and
/// panics, saying the step is not idempotent and naming what it emitted,
/// unless it emits nothing and continues nowhere. Returns the
/// instance after the first run. A step must be idempotent on its own
/// result, or a chain through it never settles.
pub fn assert_step_idempotent<A, S, O, E>(instance: A, step: S, ctx: &Context) -> A
where
    A: Domain<State = A>,
    A::Event: fmt::Debug,
    S: Fn(&A, &Context) -> Result<O, E>,
    O: Emits<A>,
    E: Fails<A> + fmt::Debug,
{
    let mut instance = instance;
    let first = match step(&instance, ctx) {
        Ok(first) => first.into_emit(),
        Err(rejection) => panic!("the step refused: {rejection:?}"),
    };
    let mut emit = first;
    for _ in 0..=STEP_LIMIT {
        for event in std::mem::take(&mut emit.events) {
            A::apply(&mut instance, &event);
        }
        let Some(next) = emit.next.take() else {
            break;
        };
        emit = match next(&instance, ctx) {
            Ok(emit) => emit,
            Err(rejection) => panic!("the step's continuation refused: {rejection}"),
        };
    }
    let again = match step(&instance, ctx) {
        Ok(again) => again.into_emit(),
        Err(rejection) => panic!("the step refused on its own result: {rejection:?}"),
    };
    if !again.events.is_empty() || again.next.is_some() {
        panic!(
            "the step is not idempotent: re-run on its own result, it emitted {:?}{}",
            again.events,
            if again.next.is_some() {
                " and continued"
            } else {
                ""
            }
        );
    }
    instance
}

// ---------------------------------------------------------------- the glue
//
// What angzarr-embedded-macros' expansion calls, and a hand-written domain,
// policy or projector calls the same way: stable, public API.

/// `value` packed as its wire message: an `Any` whose type URL is its
/// wire message's (`/package.Name`). What a hand-written payload enum's
/// [`Payload::to_any`](super::Payload::to_any) does for a domain type.
pub fn pack_message<T: DomainMessage>(value: &T) -> Any {
    super::payload::pack(&value.to_wire())
}

/// `T`'s type URL, its wire message's: `/package.Name`. What a
/// hand-written payload enum's
/// [`Payload::type_urls`](super::Payload::type_urls) lists.
pub fn type_url_of<T: DomainMessage>() -> String {
    format!("/{}", <T::Wire as prost::Name>::full_name())
}

/// `any` as a `T`, if it is one: `Ok(None)` for a payload of another
/// type, so a caller tries each of its types in turn; a payload of `T`'s
/// type whose bytes do not decode is [`PayloadError::Decode`], and one
/// that decodes but is not a valid `T` ([`DomainMessage::from_wire`]
/// refused it) is [`PayloadError::Invalid`]. What a hand-written payload
/// enum's [`Payload::from_any`](super::Payload::from_any), a policy's
/// `on_event` and a projector's `project` decode with.
pub fn decode_message<T: DomainMessage>(any: &Any) -> Result<Option<T>, PayloadError> {
    if super::payload::type_name(&any.type_url) != <T::Wire as prost::Name>::full_name() {
        return Ok(None);
    }
    let wire = <T::Wire as prost::Message>::decode(&any.value[..]).map_err(|error| {
        PayloadError::Decode {
            type_url: any.type_url.clone(),
            error,
        }
    })?;
    T::from_wire(wire)
        .map(Some)
        .map_err(|error| PayloadError::Invalid {
            type_url: any.type_url.clone(),
            reason: Report::of(&error),
        })
}

/// The full name of angzarr's `CommandRefused`.
fn command_refused() -> String {
    <v1::CommandRefused as prost::Name>::full_name()
}

/// The type URL of angzarr's `CommandRefused`, the pass-through refusal
/// fact: what a hand-written fact payload enum with refusal handlers
/// lists beside its own types.
pub fn command_refused_type_url() -> String {
    format!("/{}", command_refused())
}

/// `any`, angzarr's `CommandRefused`, as the refusal of a `C`, if it is
/// one: `Ok(None)` for a payload of another type, or for the refusal of
/// another command; a `CommandRefused`, or its command of `C`'s type,
/// that does not decode is the error. What a hand-written fact payload
/// enum decodes a refusal fact with.
pub fn decode_command_refused<C: DomainMessage>(
    any: &Any,
) -> Result<Option<CommandRefused<C>>, PayloadError> {
    if super::payload::type_name(&any.type_url) != command_refused() {
        return Ok(None);
    }
    let wire = <v1::CommandRefused as prost::Message>::decode(&any.value[..]).map_err(|error| {
        PayloadError::Decode {
            type_url: any.type_url.clone(),
            error,
        }
    })?;
    let book = wire.command.unwrap_or_default();
    let Some(command) = command_payload(&book) else {
        return Ok(None);
    };
    let Some(command) = decode_message::<C>(command)? else {
        return Ok(None);
    };
    Ok(Some(CommandRefused {
        command,
        target: book.cover.unwrap_or_default(),
        refusal: wire.refusal,
        trigger: wire.trigger.unwrap_or_default(),
    }))
}

/// The refused command's type URL in `any`, angzarr's `CommandRefused`;
/// `None` for another payload, or one that does not decode. What a
/// hand-written fact payload enum names in its
/// [`PayloadError::UnknownType`] for a refusal it has no handler for, so
/// the error says which command's refusal it was.
pub fn refused_command_type_url(any: &Any) -> Option<String> {
    if super::payload::type_name(&any.type_url) != command_refused() {
        return None;
    }
    let wire = <v1::CommandRefused as prost::Message>::decode(&any.value[..]).ok()?;
    command_payload(wire.command.as_ref()?).map(|payload| payload.type_url.clone())
}

/// A command book's payload, if it holds a command.
fn command_payload(book: &angzarr_proto::v1::CommandBook) -> Option<&Any> {
    match book.page.as_ref()?.body.as_ref()? {
        angzarr_proto::v1::command_page::Body::Command(command) => command.payload.as_ref(),
        angzarr_proto::v1::command_page::Body::Fact(_) => None,
    }
}

/// A marked-up domain's handlers, as angzarr-router dispatches them: what
/// `#[aggregate]` and `#[process_manager]` generate, each marked handler
/// registered on the router's table for the domain (`on_command`,
/// `on_fact_with_context`, `on_refused`).
pub trait Routed: Domain {
    /// `table`, with every marked handler registered on it.
    fn route(
        table: angzarr_router::domain::DomainDispatch<Self::State>,
    ) -> angzarr_router::domain::DomainDispatch<Self::State>;
}

/// The context a handler reads: the router's, its instant included.
fn context_of(cctx: angzarr_router::domain::CommandContext) -> Context {
    Context { command: cctx }
}

/// The input `any` as a `T`, or the router's ROUTER_REASON_ANY_DECODE_FAILED for a
/// payload that is not one: malformed input, a bug (INTERNAL), never a
/// refusal (decision 14.37). Bytes that do not decode carry prost's text
/// as `ext_err_txt`; a payload of another type, or not a valid `T`, its
/// type alone.
fn input<T: DomainMessage>(any: &Any) -> Result<T, angzarr_router::error::HandlerError> {
    use angzarr_router::error::{extras, CodedError, GrpcCode, HandlerError, RouterReason};
    let message_type = || super::payload::type_name(&any.type_url).to_string();
    match decode_message::<T>(any) {
        Ok(Some(value)) => Ok(value),
        Err(PayloadError::Decode { error, .. }) => Err(HandlerError::Coded(
            CodedError::undecodable(&message_type(), &error),
        )),
        Ok(None) | Err(_) => Err(HandlerError::Coded(CodedError {
            code: RouterReason::AnyDecodeFailed.as_str_name().to_string(),
            extras: [(extras::MESSAGE_TYPE.to_string(), message_type())].into(),
            grpc: GrpcCode::Internal,
            refusal: None,
        })),
    }
}

/// `events`, packed, as the book a handler answers the router with.
fn book<A: Domain>(events: Vec<A::Event>) -> EventBook {
    EventBook {
        pages: events
            .iter()
            .map(|event| angzarr_proto::v1::EventPage {
                payload: Some(event.to_any()),
                ..Default::default()
            })
            .collect(),
        ..EventBook::default()
    }
}

/// `events` as the router records a fact: the first event as the record,
/// the rest as its flags; nothing, as the record of no event.
fn fact_record<A: Domain>(events: Vec<A::Event>) -> angzarr_router::domain::FactRecord {
    let mut events = events.iter().map(|event| event.to_any());
    let fact = events.next().unwrap_or_else(|| Any {
        type_url: super::route::NOTHING.to_string(),
        value: Vec::new(),
    });
    angzarr_router::domain::FactRecord {
        fact,
        flags: events.collect(),
    }
}

/// Runs a command handler as ONE decision, for angzarr-router's
/// `DomainDispatch::on_command` (a [`Routed`] domain's table): decodes
/// `any` as the command `C` (a payload that is not one is the router's
/// ROUTER_REASON_ANY_DECODE_FAILED, INTERNAL), calls `handler` with the
/// instance, the command and the [`Context`] made of `cctx`, then applies
/// its events, in order, to a working copy of `state` and runs each
/// continuation step ([`Emit::then`]) on that copy, until none is left.
/// The answer is every event, as the book the router records. A
/// rejection, the handler's or any step's, refuses the whole decision as
/// the router carries a business refusal, under its status; more than
/// [`STEP_LIMIT`] steps is
/// [`KernelError::TooManySteps`](super::KernelError::TooManySteps), with
/// nothing committed.
///
/// What `#[handles_cmd]` registers; a hand-written [`Routed`] domain
/// registers its own handler with it, as `#[aggregate]` would.
pub fn run_command<A, R, C, O, E>(
    any: &Any,
    state: &A,
    cctx: angzarr_router::domain::CommandContext,
    handler: impl FnOnce(&A, C, &Context) -> Result<O, E>,
) -> Result<Option<EventBook>, angzarr_router::error::HandlerError>
where
    A: Domain<State = A, Rejection = Rejection<R>> + Clone,
    R: angzarr_diagnostic::Diagnostic,
    C: DomainMessage,
    O: Emits<A>,
    E: Fails<A>,
{
    let fail = super::route::failed::<R>;
    let ctx = context_of(cctx);
    let command = input::<C>(any)?;
    let mut emit = handler(state, command, &ctx)
        .map_err(|e| fail(e.into_failed()))?
        .into_emit();
    let mut working = state.clone();
    let mut events = Vec::new();
    let mut steps = 0;
    loop {
        for event in std::mem::take(&mut emit.events) {
            A::apply(&mut working, &event);
            events.push(event);
        }
        let Some(next) = emit.next.take() else {
            return Ok(Some(book::<A>(events)));
        };
        steps += 1;
        if steps > STEP_LIMIT {
            return Err(angzarr_router::error::HandlerError::Coded(
                super::route::internal(
                    super::route::TOO_MANY_STEPS,
                    [("limit".to_string(), STEP_LIMIT.to_string())],
                ),
            ));
        }
        emit = next(&working, &ctx).map_err(fail)?;
    }
}

/// Runs a fact handler as ONE decision, for angzarr-router's
/// `DomainDispatch::on_fact_with_context`: decodes `any` as the fact `F`
/// (a payload that is not one is ROUTER_REASON_ANY_DECODE_FAILED,
/// INTERNAL), calls `handler`, then applies its events and runs its
/// continuation steps ([`Record::then`]) on a working copy of `state`, as
/// [`run_command`] does. A fact is never refused, so `handler` returns no
/// `Result`; more than [`STEP_LIMIT`] steps is a bug, which panics: the
/// router catches it, and the kernel dead-letters it.
///
/// What `#[handles_fact]` registers.
pub fn run_fact<A, F, Rr>(
    any: &Any,
    state: &A,
    cctx: angzarr_router::domain::CommandContext,
    handler: impl FnOnce(&A, F, &Context) -> Rr,
) -> Result<angzarr_router::domain::FactRecord, angzarr_router::error::HandlerError>
where
    A: Domain<State = A> + Clone,
    F: DomainMessage,
    Rr: Records<A>,
{
    let ctx = context_of(cctx);
    let fact = input::<F>(any)?;
    Ok(fact_record::<A>(record(
        state,
        &ctx,
        handler(state, fact, &ctx),
    )))
}

/// Runs a passed-through refusal's handler, for angzarr-router's
/// `DomainDispatch::on_refused` of a `C`: `refused` (angzarr's
/// `CommandRefused`) as a [`CommandRefused<C>`] (its command not a `C`
/// is ROUTER_REASON_ANY_DECODE_FAILED, INTERNAL), handed to `handler` with
/// the [`Context`] of the refusal fact, and its record run as
/// [`run_fact`] runs one. The handler's error is a bug, the policy's
/// failure, as the router carries it
/// ([`StepError::PolicyFailed`](super::StepError::PolicyFailed)).
///
/// What `#[on_refused(C)]` on a domain registers.
pub fn run_on_refused<A, C, Rr, E>(
    refused: &v1::CommandRefused,
    state: &A,
    cctx: CommandContext,
    handler: impl FnOnce(&A, CommandRefused<C>, &Context) -> Result<Rr, E>,
) -> Result<angzarr_router::domain::FactRecord, angzarr_router::error::HandlerError>
where
    A: Domain<State = A> + Clone,
    C: DomainMessage,
    Rr: Records<A>,
    E: angzarr_diagnostic::Diagnostic,
{
    let ctx = context_of(cctx);
    let book = refused.command.clone().unwrap_or_default();
    let command = input::<C>(command_payload(&book).unwrap_or(&Any::default()))?;
    let refused = CommandRefused {
        command,
        target: book.cover.unwrap_or_default(),
        refusal: refused.refusal.clone(),
        trigger: refused.trigger.clone().unwrap_or_default(),
    };
    let recorded =
        handler(state, refused, &ctx).map_err(|failure| super::route::policy_failed(&failure))?;
    Ok(fact_record::<A>(record(state, &ctx, recorded)))
}

/// A fact's record run as one decision on a working copy of `state`.
fn record<A, Rr>(state: &A, ctx: &Context, recorded: Rr) -> Vec<A::Event>
where
    A: Domain<State = A> + Clone,
    Rr: Records<A>,
{
    let mut working = state.clone();
    let mut events = Vec::new();
    let mut record = recorded.into_record();
    let mut steps = 0;
    loop {
        for event in std::mem::take(&mut record.events) {
            A::apply(&mut working, &event);
            events.push(event);
        }
        let Some(next) = record.next.take() else {
            return events;
        };
        steps += 1;
        assert!(
            steps <= STEP_LIMIT,
            "the fact on stream {} ran more than its limit of {STEP_LIMIT} continuation steps",
            super::error::cover_text(&ctx.command.cover.clone().unwrap_or_default())
        );
        record = next(&working, ctx);
    }
}

/// angzarr's pass-through refusal fact for `refusal`, packed:
/// `CommandRefused { command, refusal, trigger }`, the response's refusal
/// object still packed (none for a message-only refusal) and the trigger
/// the triggering event's `EventRef`. What
/// [`Policy::on_refused`] answers by default,
/// and what a policy that translates some refusals answers for the rest.
pub fn pass_through(refusal: &v1::Refusal) -> Any {
    let trigger = refusal.event.as_ref().map(|book| EventRef {
        stream: book.cover.clone(),
        sequence: book.pages.first().map_or(0, |page| page.sequence),
    });
    super::payload::pack(&v1::CommandRefused {
        command: refusal.command.clone(),
        refusal: refusal
            .response
            .as_ref()
            .and_then(|r| match r.reason.as_ref()? {
                v1::refusal_response::Reason::Refusal(object) => Some(object.clone()),
                v1::refusal_response::Reason::Diagnostic(_) => None,
            }),
        trigger,
    })
}

/// `refusal` as a policy's translating callback for command `C` reads it,
/// a [`Refusal<C>`]: `Ok(None)` if the refused command is not a `C`; one
/// of `C`'s type that does not decode is the error, a bug (the policy's
/// failure), as an undecodable event in `on_event` is. What a
/// [`Policy::on_refused`] that translates
/// decodes with.
pub fn decode_refusal<C: DomainMessage>(
    refusal: &v1::Refusal,
) -> Result<Option<Refusal<C>>, Report> {
    let Some(payload) = refusal.command.as_ref().and_then(command_payload) else {
        return Ok(None);
    };
    let Some(command) = decode_message::<C>(payload).map_err(|error| Report::of(&error))? else {
        return Ok(None);
    };
    Ok(Some(Refusal {
        event: refusal.event.clone().unwrap_or_default(),
        command,
        response: refusal.response.clone().unwrap_or_default(),
    }))
}

/// A translating callback's answer as
/// [`Policy::on_refused`] returns it: its fact,
/// one the policy's source `S` ingests ([`Ingests`]), packed; or its
/// error as a [`Report`], whose code and args are the dead letter's
/// `cause`.
pub fn translated_fact<S: Ingests<F>, F: DomainMessage, E: angzarr_diagnostic::Diagnostic>(
    answer: Result<F, E>,
) -> Result<Any, Report> {
    answer
        .map(|fact| pack_message(&fact))
        .map_err(|error| Report::of(&error))
}

/// A policy's event handler's answer as
/// [`Policy::on_event`] returns it: its commands
/// for the target `T`, as handled (`Some`); or its error as a [`Report`],
/// whose code and args are the dead letter's `cause`.
pub fn policy_answer<T: CommandTarget, E: angzarr_diagnostic::Diagnostic>(
    answer: Result<Commands<T>, E>,
) -> Result<Option<Commands<T>>, Report> {
    answer.map(Some).map_err(|error| Report::of(&error))
}

/// A projector handler's run, boxed as [`AsyncProjector::project`]
/// returns it for the kernel to await.
pub fn projecting<'a, F>(run: F) -> Projecting<'a>
where
    F: std::future::Future<Output = Result<(), Report>> + Send + 'a,
{
    Box::pin(run)
}

/// The state descriptor of a domain whose snapshot form is `S`: `S`'s
/// type URL, supporting STRICT and AGGREGATE_HANDLES, the implicit base
/// the registry requires. What a marked-up domain's
/// [`State::descriptor`](super::State::descriptor) returns, and a
/// hand-written one with no conflict units may.
pub fn state_descriptor<S: prost::Name>() -> angzarr_proto::v1::StateDescriptor {
    angzarr_proto::v1::StateDescriptor {
        type_url: format!("/{}", S::full_name()),
        options: Some(angzarr_proto::v1::StateOptions {
            conflict_resolutions: vec![
                angzarr_proto::v1::ConflictResolution::Strict as i32,
                angzarr_proto::v1::ConflictResolution::AggregateHandles as i32,
            ],
        }),
    }
}

/// A message's protobuf package and name, as constants: what `HANDLES` and
/// `APPLIES` hold, one per marked type, read off its wire message's
/// [`prost::Name`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TypeName {
    pub package: &'static str,
    pub name: &'static str,
}

impl TypeName {
    /// `M`'s package and name.
    pub const fn of<M: prost::Name>() -> TypeName {
        TypeName {
            package: M::PACKAGE,
            name: M::NAME,
        }
    }

    /// `package.Name`: what type URLs are compared by
    /// ([`type_name`](super::type_name)).
    pub fn full_name(&self) -> String {
        match self.package.is_empty() {
            true => self.name.to_string(),
            false => format!("{}.{}", self.package, self.name),
        }
    }

    /// `/package.Name`, as angzarr writes type URLs.
    pub fn type_url(&self) -> String {
        format!("/{}", self.full_name())
    }
}

/// The snapshot form of a marked-up aggregate with no `snapshot = ..`: its
/// [`State`](super::State) impl says
/// [`SNAPSHOTS`](super::State::SNAPSHOTS)` = false`, so the kernel never
/// takes one, and this message is never written.
#[derive(Clone, PartialEq, prost::Message)]
pub struct NoSnapshot {}

impl prost::Name for NoSnapshot {
    const NAME: &'static str = "NoSnapshot";
    const PACKAGE: &'static str = "angzarr_embedded.next.markup";
}

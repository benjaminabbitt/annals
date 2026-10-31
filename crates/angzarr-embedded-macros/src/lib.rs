//! Decorators over hand-written components. [`aggregate`] and
//! [`process_manager`] generate, from the impl's [`handles_cmd`],
//! [`handles_fact`], [`on_refused`] and [`applies`] markers, the runtime
//! glue that runs a domain on angzarr-embedded (its `angzarr_embedded::next::Domain` and
//! `angzarr_embedded::next::State` impls, routing by type URL), a nameable
//! `<Name>Handler` trait, and its constants. [`policy`] generates a policy's
//! `angzarr_embedded::next::markup::Policy`, its refusal pass-through included, and
//! [`projector`] a projector's `AsyncProjector`. Hand-writing `Domain`
//! stays first-class; this is a way of writing one, not a layer over it:
//! the expansion calls only `angzarr_embedded::next::markup`'s documented,
//! stable public functions (`run_command`, `decode_message`,
//! `policy_answer`, ...), so anything it writes can be written by hand
//! (angzarr-embedded's `next/hand_written.test.rs` does).
//!
//! Every component written here assumes nothing about distribution: the
//! same aggregate, process manager, policy or projector runs in angzarr-embedded's
//! process or as its own service, with no change to its code.
//!
//! angzarr-embedded's `next/decorated.test.rs` runs the expansion; the
//! `compile_fail` examples here pin what it refuses. angzarr-embedded does not run a
//! policy yet: that comes with the kernel's committed-step dispatch.
//!
//! # Where it sits
//!
//! 1. The proto declares the messages and the component: `(component)` on
//!    the anchor, `(command){component, emits, refuses, starts}` on each
//!    command and fact, `(event){component}` on each event.
//! 2. `angzarr scaffold rust` writes the empty decorated impl, once: the
//!    `#[aggregate]` (or `#[process_manager]`) impl, one
//!    `#[handles_cmd(Cmd)]` stub per command typed from its `emits` and
//!    `refuses` (and marked `starts` where the proto says so), one
//!    `#[applies(Event)]` stub per event, `todo!()` bodies; and, for a policy,
//!    the `#[policy]` impl, with an `#[on_refused(Cmd)]` callback stub for
//!    each command angzarr-cli's refusal switch selects for translate. The file is the user's and is never
//!    overwritten; `--missing` appends only the stubs that are absent.
//!    Every handler it writes returns its events; a continuation step
//!    (below) is the user's to add, where a later event depends on an
//!    earlier one's state.
//! 3. The user writes the bodies.
//! 4. This macro generates the glue, the trait and the constants.
//!
//! A command the proto declares and the markup does not handle reaches the
//! router as a type URL it does not know, and is refused at run time as the
//! kernel's unknown type, like any type no domain reads.
//!
//! # The markup
//!
//! ```text
//! #[aggregate(domain = "wallet", snapshot = WalletSnapshot)]
//! impl Wallet {
//!     #[handles_cmd(Deposit)]
//!     fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> { .. }
//!
//!     #[handles_cmd(Hold, refuses(InsufficientFunds, WalletClosed))]
//!     fn hold(&self, cmd: Hold, ctx: &Context) -> Result<Held, Refuses<Self, Hold>> { .. }
//!
//!     #[handles_fact(Payout)] // a fact: no Result, anywhere
//!     fn payout(&self, fact: Payout, ctx: &Context) -> Record<Self> { .. }
//!
//!     #[applies(Deposited)]
//!     fn deposited(&mut self, event: Deposited) { .. }
//! }
//! ```
//!
//! - [`aggregate`], on an inherent impl:
//!   - `domain`: the `Domain::NAME`, a string literal;
//!   - `snapshot` (optional): the prost message the aggregate is
//!     snapshotted as;
//!   - `rejection` (optional): the domain's own, message-only rejection.
//!     By default it is inferred from the `Result` types of the command
//!     handlers without `refuses`, which must agree (`Infallible` when
//!     there are none);
//!   - `vis` (optional, default `pub`): the visibility of the generated
//!     `<Name>Handler` trait. An impl does not carry its type's
//!     visibility, so the scaffold writes the struct's here.
//! - [`process_manager`], on an inherent impl: the same arguments; see
//!   *Process managers*.
//! - [`handles_cmd`]`(Cmd)`, one per command (a request: refusable):
//!   `fn(&self, cmd: Cmd, ctx: &Context) -> Result<R, E>`, where `R` is an
//!   `Emits` shape: an event `E`, a tuple `(E1, E2, ..)` of up to four,
//!   `Option<E>`, `Vec<E>`, or an `Emit<Self>` with a continuation whose
//!   steps are fallible too. A variable list of different types is not a
//!   shape (deferred). `E` is the domain's own rejection, or, with
//!   `refuses(T, ..)`, `Refuses<Self, Cmd>` (see *Typed refusals*).
//!   On a process manager, `starts` marks a starting input.
//! - [`handles_fact`]`(Fact)`, one per fact (a reaction's or the outside
//!   world's: not refusable; "if it can't be refused, it's a fact"):
//!   `fn(&self, fact: Fact, ctx: &Context) -> R`, with no `Result`, where
//!   `R` is a shape or a `Record<Self>`, whose continuation steps return no
//!   `Result` either. A refusal is an event the handler records. On a
//!   process manager, `starts` marks a starting input.
//! - [`on_refused`]`(Cmd)`, one per command of another domain that a policy
//!   sent on this domain's behalf and whose refusals it handles: a fact
//!   handler, `fn(&self, refused: CommandRefused<Cmd>, ctx: &Context) ->
//!   Result<R, E>`, always a `Result`, its `ctx` the router's context of
//!   the refusal fact: it cannot refuse, but its `Err` (any `Diagnostic`)
//!   is a bug, dead-lettered as its policy's failure,
//!   `StepError::PolicyFailed` (see *Refusals*).
//!
//!   The marker says which a method is, and the macro holds the signature
//!   to it: a `#[handles_cmd]` or `#[on_refused]` without a `Result`, a
//!   `#[handles_fact]` with one, or either fact handler with an `Emit`,
//!   whose steps may refuse, does not compile.
//! - [`applies`]`(Event)`, one per event: `fn(&mut self, event: Event)`.
//!
//! There are no other markers on a domain. Stateless rules are not a
//! marker: refusing has no side effects, because the application creates
//! what a command needs inside the same unit of work its events commit in.
//!
//! ## The instance is the aggregate
//!
//! The aggregate's state is its own fields: `Domain::State` is the
//! aggregate type, and the macro generates its `State` impl. There is no
//! separate state parameter.
//!
//! The one aggregate may be, and often should be, **backed by several
//! objects**: its fields are other types with their own methods (a
//! ledger, a schedule, a set of holds), and the aggregate is the gateway
//! and orchestrator over them. Handlers ask those objects and decide;
//! appliers hand each event to the objects it changes. Only the aggregate
//! is marked up; the objects behind it know nothing of commands, events or
//! angzarr-embedded.
//!
//! ## Pure handlers
//!
//! A handler's only effect is the events it returns. It takes `&self`, so
//! it reads the instance and cannot assign to it (a handler that assigns
//! a field does not compile); it has no clock (the time is
//! `Context::recorded_at`, sampled by the application and injected) and
//! does no I/O. The **router** alone changes the instance: it applies the
//! returned events, in order, through the appliers, and records them.
//! Appliers are the only code that assigns state, and applying an event
//! never sets off further processing.
//!
//! **Interior mutability is unsupported.** A `Cell`, `RefCell`, `Mutex`,
//! atomic or anything else that lets `&self` change the instance breaks
//! event sourcing: the change is in no event, so replay and a retry do not
//! reproduce it. `&self` is the only enforcement; nothing checks for it
//! at run time.
//!
//! ## Continuations: events that depend on earlier ones
//!
//! A handler decides from the state before any of its events. Where a
//! later event must be decided from the state after an earlier one, the
//! handler returns `Emit::of(e).then(Self::step)`. A step is a pure
//! `&self` method, `fn(&self, ctx: &Context) -> Result<R, E>`, with `R`
//! any `Emits` shape and `E` anything a handler may fail with
//! (`markup::Fails`: the domain's own rejection or a `Refuses` choice, as
//! is or as a `Failed`), so a step may chain `.then(..)` again, refuse
//! under a business status, return at once (`Failed::immediate`), or fail
//! with a bug, fatal or retryable (`Failed::bug`,
//! `Failed::retryable_bug`).
//!
//! The router applies the events, calls the step on the updated instance,
//! applies what it returns, and repeats until there is no continuation.
//! All of it is **one decision**: a rejection from the handler or any step
//! refuses it all, a bug from any of them fails it all (dead-lettered as
//! the handler's bug), and every step's events are committed together, in
//! order, in one commit. A decision that runs more than
//! `angzarr_embedded::next::markup::STEP_LIMIT` steps is a bug (a step that always
//! continues): the kernel's `TooManySteps`, nothing committed. A step must
//! be idempotent on its own result (re-run there, it emits nothing);
//! `angzarr_embedded::next::markup::assert_step_idempotent` is the test helper.
//!
//! angzarr's client-side router runs continuations with the same
//! semantics: events applied, step called on the updated instance, one
//! decision, one commit, the same cap.
//!
//! ## Commands and facts
//!
//! If it can't be refused, it is a fact. A **command** (a request: a user,
//! a CLI, an API, a policy) may be refused, so its handler returns
//! `Result`, and a refusal goes back to the caller, the domain recording
//! nothing. A **fact** (a timeout, a webhook, an import, a refusal fact
//! from a policy) already happened and is never refused: its handler
//! returns no `Result` and always records an outcome, a refusal included,
//! as an event (`PayoutRefused`). A fact's step cannot fail; only a bug
//! can. angzarr-router hands a fact to its fact handler, a command to its
//! command handler, each registered on the router's table by the
//! expansion (`markup::Routed`).
//!
//! A fact's continuations are infallible too: its handler returns a
//! `Record<Self>`, `Record::of(e).then(Self::step)`, whose step is
//! `fn(&self, ctx: &Context) -> R` with no `Result`, `R` a shape or
//! another `Record`. Same semantics as `Emit`'s (applied, then the step on
//! the updated instance, one decision, the same cap), but nothing in it
//! can refuse.
//!
//! ## Typed refusals
//!
//! A command's handler may refuse with a business-specific, TYPED refusal
//! object, the markup's spelling of the proto's `(command).refuses`:
//! `#[handles_cmd(Hold, refuses(InsufficientFunds, WalletClosed))]`. Each
//! listed type is a domain type of the command's own domain (a
//! `DomainMessage`, never an event) and an `angzarr_diagnostic::Diagnostic`,
//! whose message is the refusal's. The handler's `Err` is then
//! `Refuses<Self, Hold>`, a choice the macro builds of exactly those
//! types, each `From` its type: `Err(InsufficientFunds { .. }.into())`. An
//! object not listed does not compile. To refuse under a business status
//! other than FAILED_PRECONDITION, or to fail with a bug, the `Err` is
//! `Failed<Refuses<Self, Hold>>` instead:
//! `Err(Failed::rejected(InsufficientFunds { .. }).with_status(..))`,
//! `Err(Failed::bug(&broke))`, the list still checked.
//!
//! ## Four ways to fail
//!
//! A command handler's or a step's `Failed` is one of decision 14.37's
//! four classes, and its status says which:
//!
//! - `Failed::rejected(..)` (business, fatal): a refusal under a business
//!   status (FAILED_PRECONDITION, or another of the six with
//!   `.with_status(..)`), to the caller or the issuing policy's refusal
//!   path; never retried.
//! - `Failed::immediate(&why)` (business, immediate): ABORTED under
//!   `why`'s own code, "re-read and re-decide": back to an outside caller
//!   at once; a policy's step, whose caller is the kernel, is reloaded and
//!   run again under the capped backoff, then dead-lettered.
//! - `Failed::bug(&broke)` (bug, fatal): INTERNAL ROUTER_REASON_UNHANDLED_HANDLER_ERROR,
//!   `broke`'s code as its `cause`; dead-lettered at once.
//! - `Failed::retryable_bug(status, &down)` (bug, retryable): the
//!   `markup::RetryableStatus` the handler picks, ROUTER_REASON_UNHANDLED_HANDLER_ERROR,
//!   `down`'s code as its `cause`; retried with capped backoff, then
//!   dead-lettered with its `retry_count`. The statuses are not
//!   interchangeable: `Unavailable` means resend (the dependency is down
//!   for now); `DeadlineExceeded` means resend only if the call is
//!   idempotent, since the work may have happened (a command is, by its
//!   `command_id`); `ResourceExhausted` means wait longer before resending
//!   (a quota or a capacity limit was hit).
//!
//! Neither an immediate return nor a bug ever reaches a policy's refusal
//! path.
//!
//! A command handler without `refuses` refuses with the domain's own
//! rejection, a message only. Either way the domain's `Domain::Rejection`
//! is `markup::Rejection<R>`: `Message(R)`, the domain's own rejection as
//! returned, or `Typed`, the object packed with its diagnostic. A refused
//! policy receives either as angzarr's `RefusalResponse { refusal, message
//! }` (`Rejection::response`). On angzarr the glue converts it with
//! `Into<CommandRejectedError>`, packing the object into the status
//! details.
//!
//! ## Process managers
//!
//! A process manager IS A DOMAIN: [`process_manager`] takes the same
//! arguments and markers as [`aggregate`] and generates the same
//! expansion. Its inputs are commands (which policies send it, and which it
//! may refuse) and facts (timeouts, webhooks, refusal facts); its only
//! output is its own events. It has no triggers: it subscribes to no
//! domain, and the policy for (another domain, the PM's domain) turns that
//! domain's events into commands to it. It sends no commands: to act on
//! another domain it records an event carrying the command's data, and
//! the policy for (the PM's domain, that domain) builds the command. Where a
//! workflow's logic belongs together, the PM and its policies run in one
//! process, each policy hop a step committed on its own.
//!
//! What sets it apart, `markup::ProcessManager`:
//!
//! - **Its root is the one its input names.** An input addressed to it
//!   names the instance in its cover, whatever its correlation: one
//!   instance may take inputs of several correlations, its events carrying
//!   each input's. A correlation left unset is given the root. A policy
//!   names the root of every command it sends; "the root is the
//!   correlation" is a convention it may follow.
//! - **The start rule**, the kernel's, by the descriptor's starting types.
//!   Only an input marked `starts`
//!   (`#[handles_cmd(StartBuyIn, starts)]`, `#[handles_fact(F, starts)]`)
//!   may start an instance; `STARTS` lists them. Any other input for a
//!   correlation with no instance is not delivered: a fact is skipped
//!   silently, a command refused (`COORDINATOR_REASON_PM_NO_INSTANCE`), recording nothing.
//!   `starts` on an aggregate's handler does not compile. A process
//!   manager that marks no input `starts` can never start an instance,
//!   yet angzarr-embedded does not refuse it: the check is angzarr-cli's, a
//!   generator warning, because a proc macro on stable Rust cannot warn,
//!   and a compile error would be stricter than the spec.
//!
//! Deduplication is by command id and fact id, before the handler runs,
//! as for any domain, and it has the one handler context every domain has.
//!
//! ## Policies
//!
//! ```text
//! #[policy(name = "buyin-wallet", source = BuyIn, target = Wallet)]
//! impl BuyInWallet {
//!     #[on_event(BuyInStarted)]
//!     fn started(&self, event: &BuyInStarted, ctx: &PolicyContext)
//!         -> Result<Commands<Wallet>, Infallible> { .. }
//! }
//! ```
//!
//! - [`policy`], on an inherent impl: `name` (the component its commands'
//!   ids are derived from), `source` (a `markup::EventSource`: a domain
//!   here, by its type, or a child-hosted domain's event set) and `target`
//!   (a `markup::CommandTarget`: a domain here, by its type, or a remote
//!   domain's command set): one source, one target.
//! - [`on_event`]`(Event)`, one per event of the source it handles:
//!   `fn(&self, event: &Event, ctx: &PolicyContext) -> Result<Commands<Target>, E>`,
//!   pure, each command one the target decides, with the root of its
//!   stream (for a process manager, `ctx.correlation_id()`). It always
//!   returns `Result`; `E` is any `Diagnostic` (`Infallible` for a handler
//!   that cannot fail).
//!
//! **Policies should not fail for business reasons**: they react to events
//! the application's own domains created, so an `Err` is a bug, an event
//! the policy cannot answer. So is an event of a type the policy marks
//! that does not decode. Either fails the step as
//! `StepError::PolicyFailed`, a defect (never the business `Rejected`):
//! the step is dead-lettered as a handler's bug, the error's code and
//! args its `cause`, never English, and the policy's
//! position moves past the event (unless the kernel says to stay:
//! `Kernel::on_policy_bug`).
//!
//! ## Refusals
//!
//! When a target refuses a policy's command, it records nothing. The policy
//! then delivers ONE unrefusable fact of the refusal to its OWN source
//! domain, at the exact stream of its triggering event, whose id is the
//! refused command's id; the source decides what the refusal means and
//! records its own events. By one of two paths, chosen per command:
//!
//! - **Pass-through** (the default). The policy's generated
//!   `Policy::on_refused` answers angzarr's standard `CommandRefused {
//!   command, refusal, trigger }`, the typed refusal object still packed.
//!   The source handles it in `#[on_refused(Cmd)]`, chosen by the refused
//!   command's type, unpacking the object
//!   (`refused.refusal::<InsufficientFunds>()`); it imports the target's
//!   types to do so. Which refusals a domain handles is inferred from its
//!   `#[on_refused]` handlers (`REFUSALS` lists them): one with no handler
//!   is caught at run time: the steps before it stay committed, and the
//!   refusal fact is dead-lettered and alerted. The handler's `Err` is a
//!   bug, as a policy's own is: the step fails as `StepError::PolicyFailed`,
//!   is dead-lettered by its code and args, and the policy's position
//!   moves past the trigger. A `CommandRefused` from outside a policy is
//!   accepted when its trigger names the receiving stream.
//! - **Translate**, for each command the policy has a callback for:
//!   `#[on_refused(Hold)] fn(&self, refusal: &Refusal<Hold>, ctx:
//!   &CommandContext) -> Result<F, E>` on the policy's impl reads the typed
//!   object, and the refused command's context, whole, as its target's
//!   handler was handed it (its cover, sequences, history, resolution,
//!   conflict and identity), and returns `F`, a fact its source
//!   ingests, so the source never refers to the target's types. A callback
//!   returning anything else does not compile. Its `Err`, any `Diagnostic`,
//!   is a bug, as an `#[on_event]` handler's is, and so is a refused
//!   command of the callback's type that does not decode: the step fails as
//!   `StepError::PolicyFailed`, is dead-lettered by its code and args, and
//!   the policy's position moves past the trigger. One marker,
//!   `#[on_refused(Cmd)]`, names both the source's handler and the policy's
//!   callback.
//!
//! The callback's PRESENCE is the selection: nothing else marks the mode,
//! and `TRANSLATES` lists the commands the policy has callbacks for. It is
//! fixed at compile time, as angzarr-cli's refusal switch is angzarr's:
//! in Rust the switch means the scaffold writes the callback stub, or does
//! not. Nothing about it is visible at run time beyond which fact arrives.
//!
//! The trade-off: deleting a policy's callback
//! silently reverts that command to pass-through. The source then receives
//! `CommandRefused`, and if it has no `#[on_refused]` handler for that
//! command, the refusal is a run-time dead letter, not a compile error.
//!
//! ## Context, rejections and domain types
//!
//! Every domain handler and step takes the one `Context`: the instant its
//! events will be recorded at (`ctx.recorded_at()`), and, as its public
//! field `ctx.command`, angzarr-router's whole `CommandContext` for the
//! input: the stream it decides (`ctx.command.cover`), the next sequence,
//! whether the stream has history, the sequence the command was issued
//! against, its conflict resolution, under AGGREGATE_HANDLES the conflict
//! to acknowledge (`ctx.command.conflict`), and the input's identity (its
//! command id, its correlation, and a fact's origin).
//!
//! Commands and events are **domain types** (angzarr-codegen's domain
//! layer), each a `DomainMessage` naming the prost message it travels as.
//! A handler never sees a wire type; a handler or applier taking one does
//! not compile.
//!
//! ## Construction, replay and snapshots
//!
//! A marked-up aggregate is registered with its factory, which is its
//! whole registration: `registry.aggregate::<Wallet>(Wallet::default)`, or
//! `registry.aggregate::<Wallet>(move || Wallet::with(config.clone()))` for
//! one that carries immutable reference configuration; a process manager
//! with `registry.process_manager::<BuyIn>(BuyIn::default)`, a policy with
//! `registry.policy(BuyInWallet)`. The kernel builds a fresh instance per
//! stream from the factory, replays the stream's events through the
//! appliers, then calls the handler on the result. No instance outlives
//! its input, and none is shared between streams.
//!
//! With `snapshot = Msg`, the user writes `From<&Wallet> for Msg` and
//! `fn restore(&mut self, snapshot: Msg) -> Result<(), E>` (`E` a
//! `Diagnostic`), and the macro generates the `State` impl from them, with
//! `Msg`'s type URL as the state's and STRICT and AGGREGATE_HANDLES as its
//! resolutions. Resuming builds a fresh instance from the factory, then
//! restores it, so the configuration the snapshot does not hold survives.
//! Without `snapshot`, the state has no snapshot form (`State::SNAPSHOTS`
//! is `false`), and every load replays from the start.
//!
//! # The expansion
//!
//! The impl above is kept as written, markers included (each is inert),
//! and gets beside it:
//!
//! ```text
//! // The command, fact and event sets, by type URL, kept out of user
//! // code: a module no path names, holding one variant per marked type.
//! mod __wallet_markup {
//!     pub enum Command { Deposit(Deposit), .. }
//!     pub enum Fact { Payout(Payout), .. } // and angzarr's CommandRefused, by
//!                                          // the refused command's type
//!     pub enum Event { Deposited(Deposited), .. }
//!     // impl Payload for each: decode by type URL (the type's
//!     // DomainMessage::Wire), then DomainMessage::from_wire.
//!     // impl From<Deposited> for Event, and so on.
//! }
//!
//! // For each command with `refuses`: the choice, `From` each listed
//! // type, `Into` the domain's rejection.
//! pub enum __WalletHoldRefuses { InsufficientFunds(InsufficientFunds), .. }
//! impl Refusing<Hold> for Wallet { type Refuses = __WalletHoldRefuses; const REFUSES = ..; }
//!
//! // What a policy is checked against.
//! impl Decides<Deposit> for Wallet {} // one per command
//! impl Ingests<Payout> for Wallet {}  // one per fact
//!
//! impl ::angzarr_embedded::next::State for Wallet {
//!     type Snapshot = WalletSnapshot; // or markup::NoSnapshot, with SNAPSHOTS = false
//!     // to_snapshot: From<&Wallet>; restore: Wallet::restore;
//!     // from_snapshot: Default, then restore
//!     ..
//! }
//!
//! impl ::angzarr_embedded::next::Domain for Wallet {
//!     type State = Self;
//!     type Command = __wallet_markup::Command;
//!     type Fact = __wallet_markup::Fact;
//!     type Event = __wallet_markup::Event;
//!     type Rejection = markup::Rejection<Refused>; // Refused inferred
//!     const NAME: &'static str = "wallet";
//!     // The crate's build version, as angzarr-build's descriptor has it.
//!     const RULES_VERSION: &'static str = env!("CARGO_PKG_VERSION");
//!
//!     fn apply(state: &mut Self, event: &Event) {
//!         match event {
//!             Event::Deposited(event) => state.deposited(event.clone()),
//!             ..
//!         }
//!     }
//! }
//!
//! // Every marked handler, registered on angzarr-router's table for the
//! // domain (`DomainDispatch<Wallet>`, which the registry builds, folding
//! // with `apply`): each command handler by its command's full name, run by
//! // `markup::run_command` (the handler's events, then each continuation on a
//! // working copy with them applied, up to STEP_LIMIT steps; a refusal as
//! // the router's business rejection, its typed object packed); each fact
//! // handler by `markup::run_fact`; each `#[on_refused(C)]` by the router's
//! // `on_refused`. (A process manager's `starts` inputs are its
//! // `markup::ProcessManager::STARTS`, not routes.)
//! impl markup::Routed for Wallet {
//!     fn route(table: DomainDispatch<Wallet>) -> DomainDispatch<Wallet> {
//!         table
//!             .on_command("toy.wallet.cmd.v1.Deposit", |any, state, cctx| {
//!                 markup::run_command::<Wallet, _, Deposit, _, _>(any, &*state, cctx, Wallet::deposit)
//!             })
//!             ..
//!     }
//! }
//!
//! // A process manager's, besides: impl ProcessManager for BuyIn { const STARTS = ..; }
//!
//! /// The domain's handler set, as a type, with the `vis` given: every
//! /// marked method's signature, implemented by delegating to it.
//! pub trait WalletHandler {
//!     fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Wallet>, Refused>;
//!     fn deposited(&mut self, event: Deposited);
//!     ..
//! }
//! impl WalletHandler for Wallet { .. }
//!
//! impl Wallet {
//!     /// The handled commands and facts, the commands whose refusals it
//!     /// handles, and the applied events, in marker order.
//!     pub const HANDLES: &'static [TypeName] = &[TypeName::of::<<Deposit as DomainMessage>::Wire>(), ..];
//!     pub const REFUSALS: &'static [TypeName] = &[..];
//!     pub const APPLIES: &'static [TypeName] = &[..];
//! }
//! ```
//!
//! A policy's expansion is its `markup::Policy` impl: `NAME`, `Source`,
//! `Target`, `EVENTS`, `TRANSLATES`, and `on_refused`, which unpacks the
//! refused command and either builds the pass-through `CommandRefused` or
//! calls the translating callback.
//!
//! **Refused by the macro**, each with a message saying why: a second
//! handler or applier for one type; a handler taking `&mut self`;
//! message-only command handlers whose `Result`s disagree on the
//! rejection; a marker's type that is not its method's parameter type; a
//! marked method whose shape is not the one above; a `refuses` type that is
//! not a domain type; `starts` on an aggregate; a policy's `#[on_refused]`
//! callback returning anything but a fact of its source; a policy handler's
//! commands for another domain than its target.
//!
//! Which of the messages is a fact is the marker's to say, and the
//! signature is checked against it. Checking the marker against the
//! message's declared kind in the proto is an open question.
//!
//! **Not on angzarr-embedded.** [`rejected`] and [`state_factory`],
//! angzarr-macros' rejection handlers and state factories, are a
//! `compile_error!` here: a refusal reaches the policy's source as a fact,
//! handled in `#[on_refused]`, and the instance is built by `Default` or
//! the registry's factory, as above.
//!
//! # Examples
//!
//! An aggregate that compiles (the control for the examples below):
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Two handlers for `SwitchOn` do not compile: which one decides is not a
//! question the markup leaves open.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on_again(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Infallible> {
//!         Ok(SwitchedOn)
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor do handlers that disagree on the rejection: the domain has one.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, WireError> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a handler taking the wire type: a prost message is not a
//! `DomainMessage`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(wire::SwitchOff)]
//!     fn switch_off(&self, _cmd: wire::SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a handler that assigns to the instance: it has `&self`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         self.on = false;
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a handler that takes `&mut self` to get round that.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&mut self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         self.on = false;
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! The handler trait has the `vis` given: a `pub(self)` trait is not
//! reachable outside its module.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! mod lamp {
//!     use super::*;
//!
//!     #[derive(Clone, Default)]
//!     pub struct Lamp {
//!         on: bool,
//!     }
//!
//!     #[aggregate(domain = "lamp", vis = pub(self))]
//!     impl Lamp {
//!         #[handles_cmd(SwitchOn)]
//!         fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!             Ok((!self.on).then_some(SwitchedOn))
//!         }
//!
//!         #[handles_cmd(SwitchOff)]
//!         fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!             Ok(SwitchedOff)
//!         }
//!
//!         #[applies(SwitchedOn)]
//!         fn on(&mut self, _event: SwitchedOn) {
//!             self.on = true;
//!         }
//!
//!         #[applies(SwitchedOff)]
//!         fn off(&mut self, _event: SwitchedOff) {
//!             self.on = false;
//!         }
//!     }
//! }
//!
//! fn handler_set<H: lamp::LampHandler>() {}
//!
//! fn main() {}
//! ```
//!
//! A fact handler returning a `Result` does not compile: a fact cannot be
//! refused, so its handler has nothing to refuse with.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage, Record};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Result<WentDark, Infallible> {
//!         Ok(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a command handler without one: a command may be refused, so
//! its handler says how.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage, Record};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Option<SwitchedOn> {
//!         (!self.on).then_some(SwitchedOn)
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor a fact handler returning an `Emit`, whose continuation steps may
//! refuse: a fact's steps are a `Record`'s, infallible.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage, Emit};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Emit<Self> {
//!         Emit::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor one `#[handles]` for both kinds: the marker says which a method is.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage, Record};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a `#[validates]` marker: there is none.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[validates(SwitchOn)]
//!     fn bright_enough(_cmd: &SwitchOn) -> Result<(), Infallible> {
//!         Ok(())
//!     }
//!
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a `#[rejected]` handler: a refusal reaches the policy's source
//! as a fact, handled in `#[on_refused]`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! use angzarr_embedded_macros::rejected;
//!
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//!
//!     #[rejected(domain = "grid", command = "Supply")]
//!     fn supply_refused(&self) {}
//! }
//! ```
//!
//! Nor does a `#[state_factory]`: the registry holds the factory.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{Context, DomainMessage};
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! use angzarr_embedded_macros::state_factory;
//!
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//!
//!     #[state_factory]
//!     fn fresh() -> Lamp {
//!         Lamp::default()
//!     }
//! }
//! ```
//!
//! A domain, a process manager and a policy that compile (the control for the
//! examples below): typed refusals, a starting input, a refusal handler and
//! a pass-through policy.
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[policy(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &PolicyContext) -> Result<Commands<Lamp>, Infallible> {
//!         Ok(Commands::none().to(ctx.correlation_id().clone(), SwitchOn))
//!     }
//! }
//! ```
//!
//! A handler refusing with an object its `refuses` does not list does not
//! compile: the choice holds only the listed types.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Fused.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor a handler with `refuses` whose `Err` is not its choice: the
//! signature is held to the marker.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Burnt> {
//!         match self.on {
//!             true => Err(Burnt),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor a `refuses` naming a wire type: a refusal object is a domain type
//! (of the command's own domain), as an event is.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(wire::Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(wire::Burnt {}.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! `starts` is a process manager's: on an aggregate's handler it does not
//! compile.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, starts, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! A process manager is a domain: it is named by `domain`, as an
//! aggregate is, not by a `name`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[process_manager(name = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//! ```
//!
//! A process manager has no triggers: `#[on]` is gone. Another domain's
//! events reach it as commands, through a policy.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! use angzarr_embedded_macros::on;
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[on(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//! ```
//!
//! Nor is there a root derived for it: its root is the one its input
//! names.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! use angzarr_embedded::next::markup::process_manager_root;
//! ```
//!
//! A refusal handler (`#[on_refused]` on a domain) is a fact handler that
//! always returns `Result`: it cannot refuse, but its `Err` is a bug, its
//! policy's failure. One returning a bare record does not compile.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//! ```
//!
//! A policy translating a command's refusals: its `#[on_refused(SwitchOn)]`
//! callback is the selection, and returns the source's own fact (the control
//! for the two below).
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[policy(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &PolicyContext) -> Result<Commands<Lamp>, Infallible> {
//!         Ok(Commands::none().to(ctx.correlation_id().clone(), SwitchOn))
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refusal: &Refusal<SwitchOn>,
//!         _ctx: &CommandContext,
//!     ) -> Result<LampLost, Infallible> {
//!         Ok(LampLost)
//!     }
//! }
//! ```
//!
//! Nor a callback returning anything but a fact its source ingests: here,
//! one of the source's events.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[policy(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &PolicyContext) -> Result<Commands<Lamp>, Infallible> {
//!         Ok(Commands::none().to(ctx.correlation_id().clone(), SwitchOn))
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refusal: &Refusal<SwitchOn>,
//!         _ctx: &CommandContext,
//!     ) -> Result<Lost, Infallible> {
//!         Ok(Lost)
//!     }
//! }
//! ```
//!
//! Nor a `translate(..)` argument: the callback's presence is the
//! selection, and nothing else marks the mode.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[policy(name = "timer-lamp", source = Timer, target = Lamp, translate(SwitchOn))]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &PolicyContext) -> Result<Commands<Lamp>, Infallible> {
//!         Ok(Commands::none().to(ctx.correlation_id().clone(), SwitchOn))
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refusal: &Refusal<SwitchOn>,
//!         _ctx: &CommandContext,
//!     ) -> Result<LampLost, Infallible> {
//!         Ok(LampLost)
//!     }
//! }
//! ```
//!
//! A policy sends to its one target: commands for another domain do not
//! compile.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use angzarr_embedded::next::markup::{
//! #     CommandContext, CommandRefused, Commands, Context, DomainMessage, PolicyContext, Record,
//! #     Refusal, Refuses,
//! # };
//! # use angzarr_embedded::next::WireError;
//! # use angzarr_embedded_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, policy,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl angzarr_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, angzarr_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(
//!         &self,
//!         _refused: CommandRefused<SwitchOn>,
//!         _ctx: &Context,
//!     ) -> Result<Record<Self>, Infallible> {
//!         Ok(Record::of(Lost))
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[policy(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &PolicyContext) -> Result<Commands<Timer>, Infallible> {
//!         Ok(Commands::none().to(ctx.correlation_id().clone(), StartTimer))
//!     }
//! }
//! ```
//!

//! # Retry
//!
//! Retrying a command whose stream moved is a decorator around the kernel,
//! not part of this markup. It re-runs the whole pipeline (a fresh
//! instance, the replay, the handler and its steps), which is safe because
//! they are pure and the router's applies are a fold of what they
//! returned.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{
    Error, Expr, FnArg, GenericArgument, Ident, ImplItem, ImplItemFn, ItemImpl, Pat, PathArguments,
    ReturnType, Signature, Token, Type, Visibility,
};

/// Marks an inherent impl as an aggregate, and generates its angzarr-embedded
/// `State` and `Domain` impls, its `<Name>Handler` trait and its
/// constants (see the crate docs).
///
/// Arguments: `domain = "<name>"` (the `Domain::NAME`); optionally
/// `snapshot = <prost message>`, `rejection = <type>` (inferred from the
/// command handlers without `refuses` when absent) and `vis =
/// <visibility>` (the trait's; `pub` when absent).
#[proc_macro_attribute]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    domain(attr.into(), item.into(), Kind::Aggregate)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks an inherent impl as a PROCESS MANAGER: a domain, marked up as an
/// aggregate is (the same arguments, markers and expansion), plus
/// `markup::ProcessManager`: only an input marked `starts` may start an
/// instance, by the kernel's start rule (see the crate docs).
#[proc_macro_attribute]
pub fn process_manager(attr: TokenStream, item: TokenStream) -> TokenStream {
    domain(attr.into(), item.into(), Kind::ProcessManager)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a command handler (fallible): `#[handles_cmd(Cmd)]`, with
/// optionally `refuses(T, ..)` (its typed refusal objects) and, on a
/// process manager, `starts`. Inert: [`aggregate`] and [`process_manager`]
/// read it.
#[proc_macro_attribute]
pub fn handles_cmd(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a fact handler (infallible: no `Result`, and a `Record`'s steps
/// none either): `#[handles_fact(Fact)]`, with optionally, on a process
/// manager, `starts`. Inert: [`aggregate`] and [`process_manager`] read
/// it.
#[proc_macro_attribute]
pub fn handles_fact(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// On a domain: the handler of the refusal of the policy command `Cmd` it
/// caused, passed through (`#[on_refused(Cmd)]`, a fact handler of
/// `markup::CommandRefused<Cmd>` that always returns `Result`, its `Err` a
/// bug, its policy's failure). On a policy: the translating
/// callback for `Cmd`, returning a fact of the source. Inert: the
/// component's macro reads it.
#[proc_macro_attribute]
pub fn on_refused(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an event applier. Inert: [`aggregate`] and [`process_manager`]
/// read it.
#[proc_macro_attribute]
pub fn applies(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// angzarr-macros' rejection handler, which angzarr-embedded refuses: a refusal
/// reaches its policy's source as a fact, handled in `#[on_refused]`.
#[proc_macro_attribute]
pub fn rejected(_attr: TokenStream, _item: TokenStream) -> TokenStream {
    Error::new(
        Span::call_site(),
        "angzarr-embedded has no #[rejected]: a refusal reaches the policy's source as a fact, \
         handled in #[on_refused(Cmd)]",
    )
    .into_compile_error()
    .into()
}

/// angzarr-macros' state factory, which angzarr-embedded refuses: the registry holds
/// the factory.
#[proc_macro_attribute]
pub fn state_factory(_attr: TokenStream, _item: TokenStream) -> TokenStream {
    Error::new(
        Span::call_site(),
        "angzarr-embedded has no #[state_factory]: register the factory, \
         `registry.aggregate::<A>(factory)`",
    )
    .into_compile_error()
    .into()
}

/// Marks an inherent impl as a POLICY: `#[policy(name = "..", source = <domain>,
/// target = <domain>)]` (see the crate docs), its source any
/// `markup::EventSource` (a domain here, by its type, or a child-hosted
/// domain's event set) and its target any `markup::CommandTarget` (a
/// domain here, by its type, or a remote domain's command set), with no
/// stand-in aggregate for either. Generates `markup::Policy`:
/// its refusal pass-through, and translate for each command its
/// `#[on_refused]` callbacks name.
///
/// Policies should not fail for business reasons: they react to events the
/// application's own domains created, so an `Err` from an `#[on_event]`
/// handler (or a marked event that does not decode) is a bug, dead-lettered
/// as `StepError::PolicyFailed`.
#[proc_macro_attribute]
pub fn policy(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow(attr.into(), item.into(), Flow::Policy)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a policy's handler for one event of its source:
/// `fn(&self, event: &E, ctx: &PolicyContext) -> Result<Commands<Target>, Err>`,
/// always a `Result`, its `Err` any `Diagnostic` and a bug. Inert:
/// [`policy`] reads it.
#[proc_macro_attribute]
pub fn on_event(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an inherent impl as a PROJECTOR, the one impure kind:
///
/// ```text
/// #[projector(name = "balances", sources = [Wallet, "ledger"])]
/// impl Balances {
///     #[projects(Deposited)]
///     async fn deposited(&mut self, event: &Deposited, ctx: &ProjectionContext) -> Result<(), Report> { .. }
/// }
/// ```
///
/// One or more source domains, `sources = [..]`, each a domain type (its
/// `Domain::NAME`) or a domain's name as a string (a domain with no Rust
/// type here, a child's): `AsyncProjector::SOURCES`, in that order. ONE
/// instance per process, registered with its ports: `registry.projector(Balances::new(port))`. The kernel's async
/// path runs it AFTER COMMIT, awaited before the call returns, so a
/// failure reaches the caller as `Failure::Projection`, carrying what was
/// committed, and does not stop the other projectors on the domain.
/// Unmarked events are skipped. It runs on the async command path, the
/// only one, so nothing guards against blocking on a synchronous path.
#[proc_macro_attribute]
pub fn projector(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow(attr.into(), item.into(), Flow::Projector)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a projector's handler for one event. Inert: [`projector`] reads
/// it.
#[proc_macro_attribute]
pub fn projects(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

const MARKUP: &str = "::angzarr_embedded::next::markup";

/// `::angzarr_embedded::next::markup`, as a path.
fn markup() -> Tokens {
    MARKUP.parse().expect("a path")
}

/// Which flow [`flow`] expands.
#[derive(Clone, Copy, PartialEq)]
enum Flow {
    Policy,
    Projector,
}

/// One `key = value` argument of `#[policy]` or `#[projector]`.
struct FlowArg {
    key: Ident,
    value: Expr,
}

impl Parse for FlowArg {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        if !input.peek(Token![=]) {
            return Err(Error::new_spanned(
                key,
                "#[policy] and #[projector] take `key = value` arguments only; a policy's \
                 translate mode is its `#[on_refused(Cmd)]` callback's presence",
            ));
        }
        input.parse::<Token![=]>()?;
        Ok(FlowArg {
            key,
            value: input.parse()?,
        })
    }
}

/// What a flow's arguments are.
fn usage(flow: Flow) -> &'static str {
    match flow {
        Flow::Policy => {
            "#[policy] takes `name = \"..\"`, `source = <domain>` and `target = <domain>`"
        }
        Flow::Projector => {
            "#[projector] takes `name = \"..\"` and `sources = [<domain type or \"name\">, ..]`"
        }
    }
}

/// A marked method: its marker's name, the method, and the marker's type.
struct Marked<'a> {
    marker: &'static str,
    method: &'a ImplItemFn,
    args: Marker,
}

/// The methods of `imp` marked with one of `markers`, in impl order. A
/// method with two markers is refused.
fn marked<'a>(imp: &'a ItemImpl, markers: &[&'static str]) -> syn::Result<Vec<Marked<'a>>> {
    let mut found = Vec::new();
    for item in &imp.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let mut on = method.attrs.iter().filter_map(|a| {
            markers
                .iter()
                .find(|m| a.path().is_ident(m))
                .map(|m| (*m, a))
        });
        let Some((marker, attr)) = on.next() else {
            continue;
        };
        if let Some((_, second)) = on.next() {
            return Err(Error::new_spanned(second, "a method takes one marker"));
        }
        found.push(Marked {
            marker,
            method,
            args: attr.parse_args::<Marker>()?,
        });
    }
    Ok(found)
}

/// The expansion of a policy or a projector: the impl as given (its markers
/// inert) and its trait.
fn flow(attr: Tokens, item: Tokens, flow: Flow) -> syn::Result<Tokens> {
    let args = Punctuated::<FlowArg, Token![,]>::parse_terminated.parse2(attr)?;
    let (mut name, mut source, mut target) = (None, None, None);
    for FlowArg { key, value } in args {
        match (key.to_string().as_str(), flow) {
            ("name", _) => name = Some(value),
            ("source", Flow::Policy) | ("sources", Flow::Projector) => source = Some(value),
            ("target", Flow::Policy) => target = Some(value),
            _ => return Err(Error::new_spanned(key, usage(flow))),
        }
    }
    let name = name.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
    let source = source.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
    let imp: ItemImpl = syn::parse2(item)?;
    let self_ty = &imp.self_ty;
    let m = markup();
    let generated = match flow {
        Flow::Policy => {
            let target = target.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
            let methods = marked(&imp, &["on_event", "on_refused"])?;
            let events: Vec<_> = methods.iter().filter(|x| x.marker == "on_event").collect();
            let callbacks: Vec<_> = methods
                .iter()
                .filter(|x| x.marker == "on_refused")
                .collect();
            for x in &methods {
                refuse_extras(x, false)?;
            }
            let event_types: Vec<_> = events.iter().map(|x| &x.args.ty).collect();
            let event_methods: Vec<_> = events.iter().map(|x| &x.method.sig.ident).collect();
            let commands: Vec<_> = callbacks.iter().map(|x| &x.args.ty).collect();
            let callback_methods: Vec<_> = callbacks.iter().map(|x| &x.method.sig.ident).collect();
            quote! {
                impl #m::Policy for #self_ty {
                    const NAME: &'static str = #name;
                    type Source = #source;
                    type Target = #target;
                    const EVENTS: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#event_types as #m::DomainMessage>::Wire>(),)*
                    ];
                    const TRANSLATES: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#commands as #m::DomainMessage>::Wire>(),)*
                    ];
                    const ANSWERS_REFUSALS: bool = true;

                    fn on_refused(
                        &self,
                        refusal: &#m::v1::Refusal,
                        ctx: &#m::CommandContext,
                    ) -> ::core::result::Result<::angzarr_embedded::next::Any, #m::Report> {
                        #(
                            if let ::core::option::Option::Some(refusal) =
                                #m::decode_refusal::<#commands>(refusal)?
                            {
                                return #m::translated_fact::<<Self as #m::Policy>::Source, _, _>(
                                    <#self_ty>::#callback_methods(self, &refusal, ctx),
                                );
                            }
                        )*
                        let _ = ctx;
                        ::core::result::Result::Ok(#m::pass_through(refusal))
                    }

                    fn on_event(
                        &self,
                        event: &::angzarr_embedded::next::Any,
                        ctx: &#m::PolicyContext,
                    ) -> ::core::result::Result<
                        ::core::option::Option<#m::Commands<<Self as #m::Policy>::Target>>,
                        #m::Report,
                    > {
                        #(
                            match #m::decode_message::<#event_types>(event) {
                                ::core::result::Result::Ok(::core::option::Option::Some(event)) => {
                                    return #m::policy_answer::<<Self as #m::Policy>::Target, _>(
                                        <#self_ty>::#event_methods(self, &event, ctx),
                                    );
                                }
                                ::core::result::Result::Ok(::core::option::Option::None) => {}
                                ::core::result::Result::Err(error) => {
                                    return ::core::result::Result::Err(#m::Report::of(&error));
                                }
                            }
                        )*
                        let _ = (event, ctx);
                        ::core::result::Result::Ok(::core::option::Option::None)
                    }
                }
            }
        }
        Flow::Projector => {
            let sources = projector_sources(&source)?;
            let methods = marked(&imp, &["projects"])?;
            for x in &methods {
                refuse_extras(x, false)?;
            }
            let types: Vec<_> = methods.iter().map(|x| &x.args.ty).collect();
            let names: Vec<_> = methods.iter().map(|x| &x.method.sig.ident).collect();
            quote! {
                impl #m::AsyncProjector for #self_ty {
                    const NAME: &'static str = #name;
                    const SOURCES: &'static [&'static str] = &[#(#sources,)*];
                    const PROJECTS: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#types as #m::DomainMessage>::Wire>(),)*
                    ];

                    fn project<'a>(
                        &'a mut self,
                        event: &'a ::angzarr_embedded::next::Any,
                        ctx: &'a #m::ProjectionContext,
                    ) -> ::core::option::Option<#m::Projecting<'a>> {
                        #(
                            match #m::decode_message::<#types>(event) {
                                ::core::result::Result::Ok(::core::option::Option::Some(event)) => {
                                    return ::core::option::Option::Some(#m::projecting(
                                        async move { <#self_ty>::#names(self, &event, ctx).await },
                                    ));
                                }
                                ::core::result::Result::Err(error) => {
                                    return ::core::option::Option::Some(#m::projecting(
                                        async move { ::core::result::Result::Err(#m::Report::of(&error)) },
                                    ));
                                }
                                ::core::result::Result::Ok(::core::option::Option::None) => {}
                            }
                        )*
                        let _ = (event, ctx);
                        ::core::option::Option::None
                    }
                }
            }
        }
    };
    Ok(quote! {
        #imp

        #generated
    })
}

/// A projector's `sources = [..]`, each as the name it is: a string as
/// written, a domain type as its `Domain::NAME`. Anything else, and an
/// empty list, is refused.
fn projector_sources(sources: &Expr) -> syn::Result<Vec<Tokens>> {
    let Expr::Array(array) = sources else {
        return Err(Error::new_spanned(sources, usage(Flow::Projector)));
    };
    if array.elems.is_empty() {
        return Err(Error::new_spanned(
            sources,
            "#[projector] projects at least one source domain",
        ));
    }
    array
        .elems
        .iter()
        .map(|source| match source {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(name),
                ..
            }) => Ok(quote!(#name)),
            Expr::Path(domain) => Ok(quote!(<#domain as ::angzarr_embedded::next::Domain>::NAME)),
            other => Err(Error::new_spanned(other, usage(Flow::Projector))),
        })
        .collect()
}

/// Which domain [`domain`] expands.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Aggregate,
    ProcessManager,
}

/// One `key = value` argument of `#[aggregate]` or `#[process_manager]`.
enum Arg {
    Domain(Expr),
    Snapshot(Type),
    Rejection(Type),
    Vis(Visibility),
}

impl Parse for Arg {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        match key.to_string().as_str() {
            "domain" => Ok(Arg::Domain(input.parse()?)),
            "snapshot" => Ok(Arg::Snapshot(input.parse()?)),
            "rejection" => Ok(Arg::Rejection(input.parse()?)),
            "vis" => Ok(Arg::Vis(input.parse()?)),
            _ => Err(Error::new_spanned(
                key,
                "takes `domain = \"..\"`, and optionally `snapshot = <message>`, \
                 `rejection = <type>` and `vis = <visibility>`",
            )),
        }
    }
}

/// A marker's arguments: `#[handles_cmd(Cmd, starts, refuses(T, ..))]`,
/// every one after the type optional.
struct Marker {
    ty: Type,
    starts: Option<Ident>,
    refuses: Vec<Type>,
    refuses_key: Option<Ident>,
}

impl Parse for Marker {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ty: Type = input.parse()?;
        let mut marker = Marker {
            ty,
            starts: None,
            refuses: Vec::new(),
            refuses_key: None,
        };
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "starts" => marker.starts = Some(key),
                "refuses" => {
                    let types;
                    syn::parenthesized!(types in input);
                    marker
                        .refuses
                        .extend(Punctuated::<Type, Token![,]>::parse_terminated(&types)?);
                    marker.refuses_key = Some(key);
                }
                _ => return Err(Error::new_spanned(
                    key,
                    "a handler marker takes its type, then optionally `starts` and `refuses(..)`",
                )),
            }
        }
        Ok(marker)
    }
}

/// Refuse `starts` (unless `starts_allowed`) and `refuses` where they mean
/// nothing.
fn refuse_extras(x: &Marked<'_>, starts_allowed: bool) -> syn::Result<()> {
    let starts_allowed = starts_allowed && matches!(x.marker, "handles_cmd" | "handles_fact");
    if let (Some(starts), false) = (&x.args.starts, starts_allowed) {
        return Err(Error::new_spanned(
            starts,
            match x.marker {
                "handles_cmd" | "handles_fact" => {
                    "`starts` is a process manager's: only its inputs start an instance; \
                     an aggregate's handler takes none"
                }
                _ => "`starts` marks a process manager's command or fact handler only",
            },
        ));
    }
    if let (Some(refuses), true) = (&x.args.refuses_key, x.marker != "handles_cmd") {
        return Err(Error::new_spanned(
            refuses,
            "`refuses` names a command handler's typed refusal objects: a fact is never refused",
        ));
    }
    Ok(())
}

/// The last segment of a type's path.
fn last_ident(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(path) => path.path.segments.last().map(|s| &s.ident),
        _ => None,
    }
}

/// The outer type's name and its generic arguments, for a signature's
/// return type: `Result<A, B>` is `("Result", [A, B])`.
fn returned(sig: &Signature) -> Option<(String, Vec<Type>)> {
    let ReturnType::Type(_, ty) = &sig.output else {
        return None;
    };
    let Type::Path(path) = &**ty else {
        return None;
    };
    let last = path.path.segments.last()?;
    let args = match &last.arguments {
        PathArguments::AngleBracketed(args) => args
            .args
            .iter()
            .filter_map(|a| match a {
                GenericArgument::Type(t) => Some(t.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Some((last.ident.to_string(), args))
}

/// The domain's own rejection a handler's `Err` names: `R` of a
/// `Failed<R>`, else the type itself.
fn own_rejection(error: Type) -> Type {
    let Type::Path(path) = &error else {
        return error;
    };
    let Some(last) = path.path.segments.last() else {
        return error;
    };
    match (&last.arguments, last.ident == "Failed") {
        (PathArguments::AngleBracketed(args), true) if args.args.len() == 1 => {
            match args.args.first() {
                Some(GenericArgument::Type(own)) => own.clone(),
                _ => error,
            }
        }
        _ => error,
    }
}

/// The receiver a method takes: `Some(true)` for `&mut self`, `Some(false)`
/// for `&self`, `None` for anything else.
fn receiver(sig: &Signature) -> Option<bool> {
    match sig.inputs.first()? {
        FnArg::Receiver(r) if r.reference.is_some() => Some(r.mutability.is_some()),
        _ => None,
    }
}

/// Hold each marked method's signature to its marker (see the crate docs).
fn check_signature(x: &Marked<'_>) -> syn::Result<()> {
    let sig = &x.method.sig;
    let fail = |message: &str| Err(Error::new_spanned(sig, message));
    match x.marker {
        "applies" => {
            if receiver(sig) != Some(true) {
                return fail("an applier takes `&mut self`: it assigns the event's state");
            }
            return Ok(());
        }
        _ => {
            if receiver(sig) != Some(false) {
                return fail(
                    "a handler takes `&self`: it decides, and only the router's appliers \
                     change the instance",
                );
            }
        }
    }
    let output = returned(sig);
    let outer = output.as_ref().map(|(name, _)| name.as_str());
    match x.marker {
        "handles_cmd" => {
            let Some((_, args)) = output.as_ref().filter(|(name, _)| name == "Result") else {
                return fail(
                    "a command handler returns `Result<_, Rejection>`: a command may be refused",
                );
            };
            if !x.args.refuses.is_empty()
                && args
                    .get(1)
                    .cloned()
                    .map(own_rejection)
                    .as_ref()
                    .and_then(last_ident)
                    .is_none_or(|e| e != "Refuses")
            {
                return fail(
                    "a command handler with `refuses(..)` returns `Result<_, Refuses<Self, Cmd>>` \
                     or `Result<_, Failed<Refuses<Self, Cmd>>>`",
                );
            }
            Ok(())
        }
        "on_refused" => {
            let Some((_, args)) = output.as_ref().filter(|(name, _)| name == "Result") else {
                return fail(
                    "an `#[on_refused]` handler returns `Result<Record<Self>, E>`: its `Err`, \
                     a bug, is dead-lettered as its policy's failure",
                );
            };
            match args.first().and_then(last_ident) {
                Some(ok) if ok == "Emit" => fail(
                    "an `#[on_refused]` handler records a `Record`, not an `Emit`, whose \
                     steps may refuse",
                ),
                _ => Ok(()),
            }
        }
        _ if outer == Some("Result") => {
            fail("a fact handler returns no `Result`: a fact cannot be refused")
        }
        _ if outer == Some("Emit") => {
            fail("a fact handler returns a `Record`, not an `Emit`, whose steps may refuse")
        }
        _ => Ok(()),
    }
}

/// `tokens` with every `Self` replaced by `self_ty`.
fn own_type(tokens: Tokens, self_ty: &Type) -> Tokens {
    use proc_macro2::{Group, TokenTree};
    tokens
        .into_iter()
        .map(|token| match token {
            TokenTree::Ident(ident) if ident == "Self" => quote!(#self_ty),
            TokenTree::Group(group) => {
                let mut inner = Group::new(group.delimiter(), own_type(group.stream(), self_ty));
                inner.set_span(group.span());
                TokenTree::Group(inner).into()
            }
            other => other.into(),
        })
        .collect()
}

/// `sig` with its arguments named `arg0`, `arg1`, ..: the handler trait's
/// signature, and the arguments to forward.
fn renamed(sig: &Signature) -> (Signature, Vec<Ident>) {
    let mut sig = sig.clone();
    let mut names = Vec::new();
    for (n, input) in sig.inputs.iter_mut().enumerate() {
        if let FnArg::Typed(typed) = input {
            let name = format_ident!("arg{}", n);
            *typed.pat = Pat::Verbatim(quote!(#name));
            names.push(name);
        }
    }
    (sig, names)
}

/// A payload enum: one variant per type, with its `Payload` impl. `extra`
/// are further variants (a refusal handler's), each with its type, how it
/// is decoded and how it is packed.
fn payload_set(
    name: &Ident,
    variants: &[(Ident, Type)],
    refused: &[(Ident, Type)],
    vis: &Visibility,
) -> Tokens {
    let m = markup();
    let (idents, types): (Vec<_>, Vec<_>) = variants.iter().cloned().unzip();
    let (refused_idents, refused_types): (Vec<_>, Vec<_>) = refused.iter().cloned().unzip();
    let packing = if variants.is_empty() && refused.is_empty() {
        quote!(match *self {})
    } else {
        quote! {
            match self {
                #(#name::#idents(value) => #m::pack_message(value),)*
                #(#name::#refused_idents(_, any) => ::core::clone::Clone::clone(any),)*
            }
        }
    };
    let refusal_url = (!refused.is_empty()).then(|| quote!(#m::command_refused_type_url(),));
    quote! {
        #[doc(hidden)]
        #[derive(Clone, Debug)]
        #[allow(non_camel_case_types)]
        #vis enum #name {
            #(#idents(#types),)*
            #(#refused_idents(#m::CommandRefused<#refused_types>, ::angzarr_embedded::next::Any),)*
        }

        impl ::angzarr_embedded::next::Payload for #name {
            fn to_any(&self) -> ::angzarr_embedded::next::Any {
                #packing
            }

            fn from_any(
                any: &::angzarr_embedded::next::Any,
            ) -> ::core::result::Result<Self, ::angzarr_embedded::next::PayloadError> {
                #(
                    if let ::core::option::Option::Some(value) = #m::decode_message::<#types>(any)? {
                        return ::core::result::Result::Ok(#name::#idents(value));
                    }
                )*
                #(
                    if let ::core::option::Option::Some(refused) =
                        #m::decode_command_refused::<#refused_types>(any)?
                    {
                        return ::core::result::Result::Ok(
                            #name::#refused_idents(refused, ::core::clone::Clone::clone(any)),
                        );
                    }
                )*
                ::core::result::Result::Err(::angzarr_embedded::next::PayloadError::UnknownType {
                    type_url: #m::refused_command_type_url(any)
                        .unwrap_or_else(|| ::core::clone::Clone::clone(&any.type_url)),
                })
            }

            fn type_urls() -> ::std::vec::Vec<::std::string::String> {
                ::std::vec![#(#m::type_url_of::<#types>(),)* #refusal_url]
            }
        }
    }
}

/// A variant's name for `ty`: its last segment, or `prefix` with it.
fn variant(ty: &Type, prefix: &str) -> syn::Result<Ident> {
    last_ident(ty)
        .map(|ident| format_ident!("{}{}", prefix, ident))
        .ok_or_else(|| Error::new_spanned(ty, "names no type"))
}

/// The expansion of an aggregate or a process manager (see the crate
/// docs).
fn domain(attr: Tokens, item: Tokens, kind: Kind) -> syn::Result<Tokens> {
    let args = Punctuated::<Arg, Token![,]>::parse_terminated.parse2(attr)?;
    let (mut domain, mut rejection, mut snapshot) = (None, None, None);
    let mut vis: Visibility = syn::parse_quote!(pub);
    for arg in args {
        match arg {
            Arg::Domain(value) => domain = Some(value),
            Arg::Rejection(value) => rejection = Some(value),
            Arg::Snapshot(value) => snapshot = Some(value),
            Arg::Vis(value) => vis = value,
        }
    }
    let domain = domain.ok_or_else(|| Error::new(Span::call_site(), "needs `domain = \"..\"`"))?;
    let imp: ItemImpl = syn::parse2(item)?;
    let self_ty = &imp.self_ty;
    let name = last_ident(self_ty)
        .ok_or_else(|| Error::new_spanned(self_ty, "goes on `impl <Type>`"))?
        .clone();
    let m = markup();
    let methods = marked(
        &imp,
        &["handles_cmd", "handles_fact", "on_refused", "applies"],
    )?;

    // Each signature held to its marker; one handler per type.
    let mut seen: Vec<(&'static str, String)> = Vec::new();
    for x in &methods {
        refuse_extras(x, kind == Kind::ProcessManager)?;
        check_signature(x)?;
        let set = match x.marker {
            "handles_cmd" | "handles_fact" => "input",
            other => other,
        };
        let key = (set, x.args.ty.to_token_stream().to_string());
        if seen.contains(&key) {
            return Err(Error::new_spanned(
                &x.args.ty,
                "a second handler for this type: which one decides is not a question the \
                 markup leaves open",
            ));
        }
        seen.push(key);
    }
    let of = |marker: &'static str| methods.iter().filter(move |x| x.marker == marker);

    // The domain's own rejection: given, or the message-only command
    // handlers', which must agree.
    let rejection = match rejection {
        Some(rejection) => rejection,
        None => {
            let mut found: Option<Type> = None;
            for x in of("handles_cmd").filter(|x| x.args.refuses.is_empty()) {
                let Some(error) = returned(&x.method.sig)
                    .and_then(|(_, args)| args.get(1).cloned())
                    .map(own_rejection)
                else {
                    continue;
                };
                match &found {
                    None => found = Some(error),
                    Some(first)
                        if first.to_token_stream().to_string()
                            != error.to_token_stream().to_string() =>
                    {
                        return Err(Error::new_spanned(
                            &x.method.sig.output,
                            format!(
                                "the command handlers disagree on the rejection: `{}` here, \
                                 `{}` before; the domain has one (or give `rejection = ..`)",
                                error.to_token_stream(),
                                first.to_token_stream()
                            ),
                        ));
                    }
                    Some(_) => {}
                }
            }
            found.unwrap_or_else(|| syn::parse_quote!(::core::convert::Infallible))
        }
    };
    let rejection_ty = quote!(#m::Rejection<#rejection>);

    let command_set = format_ident!("__{}Command", name);
    let fact_set = format_ident!("__{}Fact", name);
    let event_set = format_ident!("__{}Event", name);
    let typed = |marker: &'static str, prefix: &str| -> syn::Result<Vec<(Ident, Type)>> {
        of(marker)
            .map(|x| Ok((variant(&x.args.ty, prefix)?, x.args.ty.clone())))
            .collect()
    };
    let commands = typed("handles_cmd", "")?;
    let facts = typed("handles_fact", "")?;
    let refusals = typed("on_refused", "Refused")?;
    let events = typed("applies", "")?;
    let sets = [
        payload_set(&command_set, &commands, &[], &vis),
        payload_set(&fact_set, &facts, &refusals, &vis),
        payload_set(&event_set, &events, &[], &vis),
    ];

    let call = |x: &Marked<'_>| x.method.sig.ident.clone();
    let full_name =
        |ty: &Type| quote!(&#m::TypeName::of::<<#ty as #m::DomainMessage>::Wire>().full_name());
    let command_routes = of("handles_cmd").map(|x| {
        let (ty, method) = (&x.args.ty, call(x));
        let name = full_name(ty);
        quote! {
            .on_command(#name, |any, state, cctx| {
                #m::run_command::<#self_ty, _, #ty, _, _>(any, &*state, cctx, <#self_ty>::#method)
            })
        }
    });
    let fact_routes = of("handles_fact").map(|x| {
        let (ty, method) = (&x.args.ty, call(x));
        let name = full_name(ty);
        quote! {
            .on_fact_with_context(#name, |any, state, cctx| {
                #m::run_fact::<#self_ty, #ty, _>(any, state, cctx, <#self_ty>::#method)
            })
        }
    });
    let refusal_routes = of("on_refused").map(|x| {
        let (ty, method) = (&x.args.ty, call(x));
        let name = full_name(ty);
        quote! {
            .on_refused(#name, |refused, state, cctx| {
                #m::run_on_refused::<#self_ty, #ty, _, _>(refused, state, cctx, <#self_ty>::#method)
            })
        }
    });
    let apply_arms = of("applies").map(|x| {
        let (v, method) = (variant(&x.args.ty, "").expect("checked"), call(x));
        quote! {
            #event_set::#v(event) => <#self_ty>::#method(state, ::core::clone::Clone::clone(event)),
        }
    });
    let apply_body = if events.is_empty() {
        quote!(match *event {})
    } else {
        quote!(match event { #(#apply_arms)* })
    };

    // A process manager's starting inputs: its descriptor's starting
    // types, which the kernel's start rule reads.
    let process_manager = match kind {
        Kind::Aggregate => quote!(),
        Kind::ProcessManager => {
            let starts = methods
                .iter()
                .filter(|x| x.args.starts.is_some())
                .map(|x| &x.args.ty);
            quote! {
                impl #m::ProcessManager for #self_ty {
                    const STARTS: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#starts as #m::DomainMessage>::Wire>(),)*
                    ];
                }
            }
        }
    };

    let from_events = events.iter().map(|(v, ty)| {
        quote! {
            impl ::core::convert::From<#ty> for #event_set {
                fn from(event: #ty) -> Self {
                    #event_set::#v(event)
                }
            }
        }
    });
    let decides = commands
        .iter()
        .map(|(_, ty)| quote!(impl #m::Decides<#ty> for #self_ty {}));
    let ingests = facts
        .iter()
        .map(|(_, ty)| quote!(impl #m::Ingests<#ty> for #self_ty {}));
    let mut refusing = Vec::new();
    for x in of("handles_cmd").filter(|x| !x.args.refuses.is_empty()) {
        let command = &x.args.ty;
        let choice = format_ident!("__{}{}Refuses", name, variant(command, "")?);
        let variants = x
            .args
            .refuses
            .iter()
            .map(|ty| variant(ty, ""))
            .collect::<syn::Result<Vec<_>>>()?;
        let types = &x.args.refuses;
        refusing.push(quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            #vis enum #choice {
                #(#variants(#types),)*
            }

            #(
                impl ::core::convert::From<#types> for #choice {
                    fn from(object: #types) -> Self {
                        #choice::#variants(object)
                    }
                }
            )*

            impl ::core::convert::From<#choice> for #m::TypedRefusal {
                fn from(choice: #choice) -> Self {
                    match choice {
                        #(#choice::#variants(object) => #m::TypedRefusal::of(&object),)*
                    }
                }
            }

            impl ::core::convert::From<#choice> for #rejection_ty {
                fn from(choice: #choice) -> Self {
                    #m::Rejection::Typed(::core::convert::Into::into(choice))
                }
            }

            impl #m::Fails<#self_ty> for #choice {
                fn into_failed(self) -> #m::Failed<#rejection_ty> {
                    #m::Failed::rejected(self)
                }
            }

            impl #m::Refusing<#command> for #self_ty {
                type Refuses = #choice;
                const REFUSES: &'static [#m::TypeName] = &[
                    #(#m::TypeName::of::<<#types as #m::DomainMessage>::Wire>(),)*
                ];
            }
        });
    }

    // The snapshot form: the user's conversions, or none.
    let state = match snapshot {
        Some(snapshot) => quote! {
            impl ::angzarr_embedded::next::State for #self_ty {
                type Snapshot = #snapshot;

                fn to_snapshot(&self) -> #snapshot {
                    <#snapshot as ::core::convert::From<&#self_ty>>::from(self)
                }

                fn from_snapshot(
                    snapshot: #snapshot,
                ) -> ::core::result::Result<Self, #m::Report> {
                    let mut state = <#self_ty as ::core::default::Default>::default();
                    ::angzarr_embedded::next::State::restore(&mut state, snapshot)?;
                    ::core::result::Result::Ok(state)
                }

                fn restore(
                    &mut self,
                    snapshot: #snapshot,
                ) -> ::core::result::Result<(), #m::Report> {
                    <#self_ty>::restore(self, snapshot).map_err(|error| #m::Report::of(&error))
                }

                fn descriptor() -> ::angzarr_embedded::next::v1::StateDescriptor {
                    #m::state_descriptor::<#snapshot>()
                }
            }
        },
        None => quote! {
            impl ::angzarr_embedded::next::State for #self_ty {
                type Snapshot = #m::NoSnapshot;

                const SNAPSHOTS: bool = false;

                fn to_snapshot(&self) -> #m::NoSnapshot {
                    #m::NoSnapshot {}
                }

                fn from_snapshot(
                    _snapshot: #m::NoSnapshot,
                ) -> ::core::result::Result<Self, #m::Report> {
                    ::core::result::Result::Ok(<#self_ty as ::core::default::Default>::default())
                }

                fn restore(
                    &mut self,
                    _snapshot: #m::NoSnapshot,
                ) -> ::core::result::Result<(), #m::Report> {
                    ::core::result::Result::Ok(())
                }

                fn descriptor() -> ::angzarr_embedded::next::v1::StateDescriptor {
                    #m::state_descriptor::<#m::NoSnapshot>()
                }
            }
        },
    };

    // The handler trait: every marked method, forwarded.
    let handler = format_ident!("{}Handler", name);
    let (declared, forwarded): (Vec<_>, Vec<_>) = methods
        .iter()
        .map(|x| {
            let (sig, names) = renamed(&x.method.sig);
            let method = &sig.ident;
            let declared = own_type(quote!(#sig), self_ty);
            let forwarded = quote!(#declared { <#self_ty>::#method(self, #(#names),*) });
            (declared, forwarded)
        })
        .unzip();
    let constant = |marker: &'static str| {
        let types: Vec<_> = of(marker).map(|x| &x.args.ty).collect();
        quote!(&[#(#m::TypeName::of::<<#types as #m::DomainMessage>::Wire>(),)*])
    };
    let handled: Vec<_> = methods
        .iter()
        .filter(|x| x.marker == "handles_cmd" || x.marker == "handles_fact")
        .map(|x| &x.args.ty)
        .collect();
    let refusal_constant = constant("on_refused");
    let applies_constant = constant("applies");

    Ok(quote! {
        #imp

        #(#sets)*

        #state

        #(#from_events)*
        #(#decides)*
        #(#ingests)*
        #(#refusing)*

        impl ::angzarr_embedded::next::Domain for #self_ty {
            type State = Self;
            type Command = #command_set;
            type Fact = #fact_set;
            type Event = #event_set;
            type Rejection = #rejection_ty;

            const NAME: &'static str = #domain;
            const RULES_VERSION: &'static str = ::core::env!("CARGO_PKG_VERSION");

            fn apply(state: &mut Self, event: &#event_set) {
                #apply_body
            }
        }

        impl #m::Fails<#self_ty> for #rejection {
            fn into_failed(self) -> #m::Failed<#rejection_ty> {
                #m::Failed::rejected(self)
            }
        }

        impl #m::Routed for #self_ty {
            fn route(
                table: ::angzarr_embedded::next::angzarr_router::domain::DomainDispatch<Self>,
            ) -> ::angzarr_embedded::next::angzarr_router::domain::DomainDispatch<Self> {
                table
                    #(#command_routes)*
                    #(#fact_routes)*
                    #(#refusal_routes)*
            }
        }

        #process_manager

        /// The domain's handler set: every marked method.
        #vis trait #handler {
            #(#declared;)*
        }

        impl #handler for #self_ty {
            #(#forwarded)*
        }

        impl #self_ty {
            /// The handled commands and facts, in marker order.
            pub const HANDLES: &'static [#m::TypeName] =
                &[#(#m::TypeName::of::<<#handled as #m::DomainMessage>::Wire>(),)*];
            /// The commands whose refusals it handles (`#[on_refused]`),
            /// in marker order.
            pub const REFUSALS: &'static [#m::TypeName] = #refusal_constant;
            /// The applied events, in marker order.
            pub const APPLIES: &'static [#m::TypeName] = #applies_constant;
        }
    })
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;

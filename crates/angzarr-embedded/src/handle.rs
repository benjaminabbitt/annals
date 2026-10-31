//! The command handler: a command, then every policy step it sets off, each
//! committed on its own: atomicity covers one handling step, never a
//! chain.
//!
//! [`Kernel::handle`] runs the book through the five steps (validate,
//! identify, load then deduplicate, resolve, decode and decide or ingest)
//! and writes its events: ONE atomic write to the store, at the length the
//! step loaded. Then every policy of its domain runs on those events, each
//! command a step of its own in its target, written with the policy's
//! position on the source; whose events set off their own policies, stream
//! by stream in the order written, until the chain settles. The call waits
//! for it.
//!
//! - **A duplicate** (step 3 finds the command's events on its stream) is
//!   answered [`Handled::Duplicate`] with them, once its stream's policies
//!   have run whatever they still owe (a retry after a crash finishes the
//!   chain).
//! - **A failing step keeps every write before it**: nothing is rolled
//!   back. The failure goes back to the caller with the path to the step
//!   ([`Failure::Downstream`]). A refused command of a policy that does not
//!   answer refusals is dead-lettered and its position moves past it, and
//!   so is a policy's own error ([`StepError::PolicyFailed`], a defect,
//!   dead-lettered by its code and args) and a handler's bug in the step
//!   (a [`KernelError::Router`] dead-lettered by its code and args), unless
//!   the kernel keeps the position at the trigger for those two
//!   ([`OnPolicyBug::Stay`]); any other bug (a malformed policy book, the
//!   depth limit) or a store failure holds the position, so
//!   [`Kernel::resume`] runs the step again, deduplicated by its derived
//!   command id.
//! - **A refused command of a policy that answers refusals** (every
//!   marked-up one) is no failure: the policy's refusal fact goes to the
//!   stream of the triggering event as a step of its own, with the refused
//!   command's id; a refusal its source has no handler for is
//!   dead-lettered.
//! - **Projections run** on what each step wrote: a [`Project`]ion route's
//!   failure is reported in [`Recorded::projector_failures`]; a marked-up
//!   projector's ([`Registry::projector`]) and a `ProjectorHandler`'s
//!   ([`Registry::projector_handler`], a projector in another process,
//!   its failures by their class: retried, then dead-lettered, or
//!   dead-lettered at once) are the caller's [`Failure::Projection`]. A
//!   failing projection never undoes a write.
//!
//! Which policies run is the registry's one list
//! ([`Registry::policy`]), in registration order, so a chain can come back
//! to a stream it already touched (A → B → A); a cascade deeper than the
//! chain's limit is a [`KernelError::ChainTooDeep`], which is how a cycle
//! that never settles ends.

use std::any::Any as AnyType;
use std::collections::VecDeque;
use std::convert::Infallible;

use angzarr_proto::v1::{
    self as v1, command_page, refusal_response, CommandBook, CommandPage, CommandRefused,
    ConflictResolution, Cover, DeliveryOutcome, EventBook, EventPage, EventRef, Fact, FactOrigin,
    Outcome,
};
use prost_types::Any;

use angzarr_proto::v1::angzarr_dead_letter::Payload;
use angzarr_router::error::RouterReason;

use super::check::{check, check_refusal, Checked, Identified, Sampled};
use super::coordinator::{
    contextual_command, decide, deduplicate, load, write_snapshot, Contextual, Deduplicated,
};
use super::dead_letter::{self, write_at_cap, Cause, Component, DeadLetterLog};
use super::domain::Domain;
use super::error::{class, Class};
use super::error::{Failure, KernelError, Step, StepError};
use super::handler::Handler;
use super::markup::{self, ProjectionContext};
use super::payload::type_name;
use super::policy::{answered, check_reactions, projection, Projection};
use super::registry::{PolicyEntry, ProjectorInstance, Registry};
use super::route::Rejected;
use super::store::{Port, Store, StoreError, StorePort};
use super::stream::{Envelope, Stream};
use super::target::{forward, Forwarded, Target};
use super::upcast::upcast_pages;
use angzarr_diagnostic::Report;
use angzarr_router::router::DynProjectorHandler;

/// How deep a chain may go before it is a [`KernelError::ChainTooDeep`],
/// unless the kernel says otherwise ([`Kernel::limit`]).
pub const DEFAULT_DEPTH_LIMIT: usize = 8;

/// The race re-run cap by default (decision 14.46): ten runs, the client
/// library's default retry attempts. A re-run happens only when another
/// write lands between a load and its append, so ten losses in a row mean
/// a stream too hot for more runs to win.
pub const DEFAULT_RACE_RUNS: u32 = 10;

/// What the application builds once and handles every command through: the
/// registry, where dead letters go, where trigger edges go, and the chain's
/// depth limit.
pub struct Kernel<'k> {
    registry: &'k Registry,
    dead_letters: &'k dyn DeadLetterLog,
    limit: usize,
    policy_bug: OnPolicyBug,
    retry: OnRetryable,
    wait: &'k dyn Wait,
}

/// What the kernel does with a step's retryable failure (decision 14.37:
/// a failure that is neither a business refusal nor a bug, the kernel's
/// [`Recovery::Retry`](super::Recovery::Retry): UNAVAILABLE, ABORTED,
/// UNIMPLEMENTED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED and the like): run
/// the step again after a backoff, up to `max_retries` times, then give it
/// up: a policy's step (no caller is left) is dead-lettered, visibly,
/// under its key; a command its caller waits for is handed back as last
/// answered, and not dead-lettered (owner ruling, 2026-10-31). The `n`th retry (from 0) waits
/// `initial_backoff` times `multiplier` to the `n`, at most `max_backoff`,
/// less a random part of it up to `jitter` (0 to 1): `base * (1 - jitter *
/// draw)`, `draw` uniform in `[0, 1)` ([`Wait::draw`]). On by default;
/// `max_retries = 0` turns it off: a retryable failure goes back to the
/// caller at once and is not dead-lettered.
///
/// The same cap bounds an AGGREGATE_HANDLES or COMMUTATIVE command's
/// re-deliveries of its widened window after a race lost at its write
/// (decision 14.45): at most `max_retries + 1` deliveries, the first
/// included. A command stamped with the head it loaded (no expected
/// sequence, or a fact) that loses such a race is re-run on the new head
/// under a cap of its own, `max_race_runs` (decision 14.46), with the same
/// backoff and jitter between runs. At either cap the command is given
/// up, [`KernelError::RetryCapExhausted`] (a fact, [`Handled::Parked`]):
/// a policy's step and a fact are dead-lettered, a command its caller
/// waits for is not. `max_race_runs = 0` turns race re-runs off, for a
/// caller that owns its retry loop: a stamped command's lost race goes
/// back to the caller at once as the store's conflict
/// ([`StoreError::Conflict`], [`Recovery::Retry`](super::Recovery::Retry);
/// on the wire UNAVAILABLE `COORDINATOR_REASON_STORE_CONFLICT`,
/// [`KernelError::as_coded`], decision 14.51),
/// not run again and not dead-lettered; a policy's step that loses it
/// holds the policy's position at the trigger, so the next catch-up runs
/// it again.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OnRetryable {
    /// Retries after the first attempt; 0 turns retrying off.
    pub max_retries: u32,
    /// Runs of a stamped command's business code, the first included,
    /// while it loses the race at its write: the race re-run cap. 0 turns
    /// race re-runs off: the first lost race goes back to the caller as the
    /// store's conflict (retryable; `COORDINATOR_REASON_STORE_CONFLICT`),
    /// not dead-lettered.
    pub max_race_runs: u32,
    /// The wait before the first retry.
    pub initial_backoff: std::time::Duration,
    /// What each retry's wait is the last one's times (at least 1).
    pub multiplier: f64,
    /// The longest wait between retries.
    pub max_backoff: std::time::Duration,
    /// The most of a wait taken off at random, as a fraction of it (0 to
    /// 1), so retries of many failures do not land together.
    pub jitter: f64,
}

impl Default for OnRetryable {
    /// Three retries, from 100 ms, doubling, at most 5 s apart, a fifth
    /// of each wait at random; [`DEFAULT_RACE_RUNS`] runs of a stamped
    /// command that keeps losing its race.
    fn default() -> Self {
        OnRetryable {
            max_retries: 3,
            max_race_runs: DEFAULT_RACE_RUNS,
            initial_backoff: std::time::Duration::from_millis(100),
            multiplier: 2.0,
            max_backoff: std::time::Duration::from_secs(5),
            jitter: 0.2,
        }
    }
}

/// How the kernel waits out a backoff, and draws its jitter: the
/// application's timer and randomness. The kernel's own
/// ([`Kernel::new`]'s) sleeps on a thread of its own and draws from the
/// standard library's hasher seeds; on wasm32, which has no threads, it
/// waits not at all, so a retry runs at once, and an application there
/// that wants the backoff injects its timer ([`Kernel::wait_with`]); a
/// test injects its own, so its waits are exact.
pub trait Wait: Send + Sync {
    /// A future that completes once `backoff` has passed.
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;

    /// A draw uniform in `[0, 1)`: the jitter's randomness.
    fn draw(&self) -> f64;
}

impl OnRetryable {
    /// The wait before retry `retry` (from 0), its jitter `draw` (in
    /// `[0, 1)`) taken off: `initial_backoff * multiplier^retry`, at most
    /// `max_backoff`, times `1 - jitter * draw`. A multiplier below 1 is 1,
    /// a jitter outside `[0, 1]` the nearer end.
    pub fn backoff(&self, retry: u32, draw: f64) -> std::time::Duration {
        let grown = self.initial_backoff.as_secs_f64()
            * self
                .multiplier
                .max(1.0)
                .powi(i32::try_from(retry).unwrap_or(i32::MAX));
        let base = grown.min(self.max_backoff.as_secs_f64());
        std::time::Duration::from_secs_f64(base * (1.0 - self.jitter.clamp(0.0, 1.0) * draw))
    }
}

/// Whether the target can spawn a thread: every one but wasm32
/// (`wasm32-unknown-unknown` panics on a spawn).
const THREADS: bool = cfg!(not(target_arch = "wasm32"));

/// The kernel's own [`Wait`] on a target with threads ([`ThreadWait`]),
/// or without them ([`NoWait`]).
fn default_wait(threads: bool) -> &'static dyn Wait {
    match threads {
        true => &ThreadWait,
        false => &NoWait,
    }
}

/// The kernel's own [`Wait`] without threads (wasm32): each wait completes
/// at once, whatever its backoff, so a retry runs straight after the
/// failure, never panicking on a thread's spawn. An application there
/// that wants the backoff waited out injects its own timer
/// ([`Kernel::wait_with`]). It draws as [`ThreadWait`] does.
struct NoWait;

impl Wait for NoWait {
    fn wait(
        &self,
        _backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        ThreadWait.draw()
    }
}

/// The kernel's own [`Wait`]: each wait on a thread of its own, which
/// wakes the waiting task when the backoff has passed (no runtime of the
/// kernel's own; not on `wasm32-unknown-unknown`, which has no threads), and
/// draws from the standard library's randomly keyed hasher.
struct ThreadWait;

impl Wait for ThreadWait {
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(Sleep {
            backoff,
            shared: None,
        })
    }

    fn draw(&self) -> f64 {
        use std::hash::{BuildHasher, Hasher};
        let bits = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        // The top 53 bits, as a fraction: uniform in [0, 1).
        (bits >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// A wait of `backoff` on a thread of its own.
struct Sleep {
    backoff: std::time::Duration,
    /// Whether the thread has woken, and the waker to wake: set at the
    /// first poll, which starts the thread.
    shared: Option<std::sync::Arc<std::sync::Mutex<(bool, std::task::Waker)>>>,
}

impl std::future::Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let lock = |shared: &std::sync::Mutex<(bool, std::task::Waker)>| {
            shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0
        };
        match &self.shared {
            Some(shared) => {
                let mut held = shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if held.0 {
                    return std::task::Poll::Ready(());
                }
                held.1 = cx.waker().clone();
                std::task::Poll::Pending
            }
            None => {
                let shared =
                    std::sync::Arc::new(std::sync::Mutex::new((false, cx.waker().clone())));
                let woken = std::sync::Arc::clone(&shared);
                let backoff = self.backoff;
                std::thread::spawn(move || {
                    std::thread::sleep(backoff);
                    let mut held = woken
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    held.0 = true;
                    held.1.wake_by_ref();
                });
                let done = lock(&shared);
                self.shared = Some(shared);
                match done {
                    true => std::task::Poll::Ready(()),
                    false => std::task::Poll::Pending,
                }
            }
        }
    }
}

/// What a policy step that hits a bug does with the policy's position on
/// its source, once the step is dead-lettered: the policy's own error
/// ([`StepError::PolicyFailed`]) and a handler's bug in the step (a
/// [`KernelError::Router`] the kernel dead-letters) alike.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OnPolicyBug {
    /// Move the position past the trigger: the dead letter is the record
    /// to replay, and [`Kernel::resume`] does not run the step again.
    #[default]
    MovePast,
    /// Keep the position at the trigger, so [`Kernel::resume`] runs the
    /// step again (and dead-letters it again while the bug stands).
    Stay,
}

/// angzarr's code for a projector failure: google.rpc.Code INTERNAL.
const INTERNAL: i32 = 13;

/// What a handled command came to (angzarr's `Outcome.delivery`).
pub enum Handled {
    /// Business code ran, and the chain committed: everything it recorded
    /// (possibly nothing: a decision with no events).
    Applied(Recorded),
    /// Step 3 found the command's events already on its stream: these, in
    /// sequence order. Nothing ran and nothing was written.
    Duplicate(EventBook),
    /// A fact the kernel gave up on, its write lost to another write to its
    /// stream until the race re-run cap: dead-lettered, nothing written
    /// (decision 14.46). A fact is never refused, so its sender is answered
    /// accepted and parked, with no events.
    Parked,
}

impl Handled {
    /// As angzarr's `Outcome`: APPLIED, with each projector failure as a
    /// `ProjectorFailure` (code INTERNAL), DUPLICATE, or PARKED for a fact
    /// given up at the race re-run cap. In process, a panicking ingest
    /// fails the call: it is never parked.
    pub fn outcome(&self) -> Outcome {
        match self {
            Handled::Applied(recorded) => Outcome {
                delivery: DeliveryOutcome::Applied as i32,
                projections: Vec::new(),
                projector_failures: recorded
                    .projector_failures
                    .iter()
                    .map(|f| v1::ProjectorFailure {
                        projector: f.projector.to_string(),
                        code: INTERNAL,
                        diagnostic: None,
                    })
                    .collect(),
            },
            Handled::Duplicate(_) => Outcome {
                delivery: DeliveryOutcome::Duplicate as i32,
                ..Outcome::default()
            },
            Handled::Parked => Outcome {
                delivery: DeliveryOutcome::Parked as i32,
                ..Outcome::default()
            },
        }
    }

    /// The events to answer with: every stream the chain appended to, or
    /// the duplicate's; none for a parked fact.
    pub fn books(&self) -> Vec<EventBook> {
        match self {
            Handled::Applied(recorded) => recorded.books.clone(),
            Handled::Duplicate(book) => vec![book.clone()],
            Handled::Parked => Vec::new(),
        }
    }
}

impl<'k> Kernel<'k> {
    /// With [`DEFAULT_DEPTH_LIMIT`].
    pub fn new(registry: &'k Registry, dead_letters: &'k dyn DeadLetterLog) -> Kernel<'k> {
        Kernel {
            registry,
            dead_letters,
            limit: DEFAULT_DEPTH_LIMIT,
            policy_bug: OnPolicyBug::default(),
            retry: OnRetryable::default(),
            wait: default_wait(THREADS),
        }
    }

    /// Retries a step's retryable failure as `retry` says, then
    /// dead-letters it ([`OnRetryable::default`] unless told otherwise;
    /// `max_retries = 0` turns it off).
    pub fn on_retryable(mut self, retry: OnRetryable) -> Kernel<'k> {
        self.retry = retry;
        self
    }

    /// Waits out each backoff, and draws its jitter, with `wait`.
    pub fn wait_with(mut self, wait: &'k dyn Wait) -> Kernel<'k> {
        self.wait = wait;
        self
    }

    /// How this kernel retries a step's retryable failure.
    pub fn retryable(&self) -> OnRetryable {
        self.retry
    }

    /// Does `policy_bug` with a policy's position when one of its steps
    /// hits a bug ([`OnPolicyBug::MovePast`] unless told otherwise).
    pub fn on_policy_bug(mut self, policy_bug: OnPolicyBug) -> Kernel<'k> {
        self.policy_bug = policy_bug;
        self
    }

    /// What this kernel does with a policy's position when one of its
    /// steps hits a bug.
    pub fn policy_bug(&self) -> OnPolicyBug {
        self.policy_bug
    }

    /// Chains at most `limit` policy steps deep.
    pub fn limit(mut self, limit: usize) -> Kernel<'k> {
        self.limit = limit;
        self
    }

    pub fn registry(&self) -> &'k Registry {
        self.registry
    }

    /// Handle one book for the `D` stream it names, then every policy step
    /// it sets off, and wait for the chain to settle (see the module docs).
    /// Each step is ONE atomic write to `store`: one stream's events, with
    /// the step's positions. The command's own failure writes nothing: a
    /// refused write is [`KernelError::Store`]. A race lost at the write
    /// (another write to the stream landed first; decisions 14.45, 14.46)
    /// is run again on the new head when the kernel stamped the command's
    /// expected sequence, under the race re-run cap; under AGGREGATE_HANDLES
    /// and COMMUTATIVE, a sender's expected sequence is re-delivered the
    /// widened window, under the retry cap; under STRICT, it is
    /// [`KernelError::Stale`]. At either cap the command is dead-lettered,
    /// [`KernelError::RetryCapExhausted`], and a fact [`Handled::Parked`]. A cover of another
    /// domain than `D` is [`KernelError::WrongDomain`]; a remote domain's
    /// command goes to its coordinator ([`Registry::remote`]). Business
    /// code stays synchronous and pure, each dispatch awaited; the future
    /// is `Send` whenever the store's are.
    pub async fn handle<D, S, Ps>(
        &self,
        store: &S,
        book: CommandBook,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<Rejected>>
    where
        D: Domain,
        S: Store,
        Ps: Project,
    {
        let checked = self.check::<D>(book)?;
        self.handle_checked(store, checked, sampled, projections)
            .await
    }

    /// Handle one book for whatever registered domain's stream it names, as
    /// [`Kernel::handle`] does: for a domain registered as a bare
    /// `CommandHandler` ([`Registry::command_handler`]), which has no
    /// typed [`Domain`].
    pub async fn handle_any<S, Ps>(
        &self,
        store: &S,
        book: CommandBook,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<Rejected>>
    where
        S: Store,
        Ps: Project,
    {
        let checked =
            check(book, self.registry).map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        self.handle_checked(store, checked, sampled, projections)
            .await
    }

    /// [`Kernel::handle`] from step 2 on.
    async fn handle_checked<S, Ps>(
        &self,
        store: &S,
        checked: Checked,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<Rejected>>
    where
        S: Store,
        Ps: Project,
    {
        let mut port = StorePort(store);
        let mut run = Run::new(self, sampled);
        let identified = checked.identify(sampled);
        let cover = identified.cover().clone();
        let mut tries = Tries::default();
        let origin = loop {
            let failure = match run.origin(&mut port, identified.clone()).await {
                Err(failure) => failure,
                answer => break answer,
            };
            match run.again(&failure, false, &mut tries, &identified).await {
                Ok(true) => {}
                Ok(false) => break Err(failure),
                Err(given_up) => break Err(given_up),
            }
        };
        let origin = match origin {
            // A fact is never refused: given up at the cap, it is parked.
            Err(Failure::Kernel(KernelError::RetryCapExhausted { .. }))
                if identified.body().is_fact() =>
            {
                return Ok(Handled::Parked)
            }
            origin => origin?,
        };
        match origin {
            Origin::Duplicate(events) => {
                // A retry settles what its own events still owe, as the
                // original would have: never the stream's older history.
                if let Some(first) = events.pages.first() {
                    run.queue
                        .push_back((cover, Vec::new(), Some(first.sequence)));
                    run.settle(&mut port).await.map_err(downstream)?;
                }
                let failures = self.run_projectors(&run.recorded.steps).await;
                if !failures.is_empty() {
                    return Err(Failure::Projection {
                        committed: run.recorded.books,
                        failures,
                    });
                }
                Ok(Handled::Duplicate(events))
            }
            Origin::Skipped => {
                self.finish(Handled::Applied(Recorded::default()), projections)
                    .await
            }
            Origin::Forwarded(Forwarded::Duplicate(events)) => Ok(Handled::Duplicate(events)),
            Origin::Forwarded(Forwarded::Applied(events)) => {
                run.answered(events);
                self.finish(Handled::Applied(run.recorded), projections)
                    .await
            }
            Origin::Wrote(head) => {
                run.queue.push_back((cover, Vec::new(), Some(head)));
                let settled = run.settle(&mut port).await.map_err(downstream);
                let recorded = run.recorded;
                settled?;
                self.finish(Handled::Applied(recorded), projections).await
            }
        }
    }

    /// Run every policy step still owed, by position: for each stream of
    /// each policy's source domain, the events from the lowest of its
    /// policies' positions on, each policy for each event it has yet to
    /// handle, each step as [`Kernel::handle`] runs them (deduplicated, so
    /// a step that landed before a crash is not repeated; one write each,
    /// its position written with it). What an application runs at start,
    /// after a crash, or after adding a policy to a domain with history.
    /// Returns what the steps recorded.
    pub async fn resume<S, Ps>(
        &self,
        store: &S,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Recorded, Failure<Infallible>>
    where
        S: Store,
        Ps: Project,
    {
        let mut port = StorePort(store);
        let mut run = Run::new(self, sampled);
        let mut sources: Vec<&str> = Vec::new();
        for policy in self.registry.all_policies() {
            if !sources.contains(&policy.source()) {
                sources.push(policy.source());
            }
        }
        for source in sources {
            let covers = port
                .covers(source)
                .await
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
            for cover in covers {
                run.queue.push_back((cover, Vec::new(), None));
            }
        }
        let settled = run.settle(&mut port).await.map_err(downstream);
        let recorded = run.recorded;
        settled?;
        match self.finish(Handled::Applied(recorded), projections).await? {
            Handled::Applied(recorded) => Ok(recorded),
            Handled::Duplicate(_) | Handled::Parked => {
                unreachable!("a resume answers what it recorded")
            }
        }
    }

    /// Run `projections`, then every marked-up projector, over what was
    /// written.
    async fn finish<R, Ps: Project>(
        &self,
        handled: Handled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<R>> {
        let handled = self.project(handled, projections);
        let (steps, books) = match &handled {
            Handled::Applied(recorded) => (recorded.steps.clone(), recorded.books.clone()),
            Handled::Duplicate(_) | Handled::Parked => return Ok(handled),
        };
        let failures = self.run_projectors(&steps).await;
        if !failures.is_empty() {
            return Err(Failure::Projection {
                committed: books,
                failures,
            });
        }
        Ok(handled)
    }

    /// Every registered projector over the `committed` events of its
    /// sources, in registration order: a marked-up one each event in stream
    /// order, a `ProjectorHandler` each book whole ([`Self::project_book`]).
    /// A projector stops at its first failure, the others go on. The
    /// failures, by projector.
    async fn run_projectors(&self, committed: &[EventBook]) -> Vec<(String, Report)> {
        let mut failures = Vec::new();
        for entry in self.registry.projectors() {
            let books: Vec<&EventBook> = committed
                .iter()
                .filter(|book| {
                    book.cover
                        .as_ref()
                        .is_some_and(|c| entry.sources.contains(&c.domain))
                })
                .collect();
            if books.iter().all(|book| book.pages.is_empty()) {
                continue;
            }
            let instance = match &entry.instance {
                ProjectorInstance::Marked(instance) => instance,
                ProjectorInstance::Handler(handler) => {
                    for book in books.into_iter().filter(|book| !book.pages.is_empty()) {
                        if let Err(error) = self.project_book(&entry.name, &**handler, book).await {
                            failures.push((entry.name.clone(), error));
                            break;
                        }
                    }
                    continue;
                }
            };
            // The one instance is out of the lock while it runs; a
            // concurrent command waits for it.
            let mut taken_instance = loop {
                let taken = instance
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                match taken {
                    Some(instance) => break instance,
                    None => Yield(false).await,
                }
            };
            'books: for book in books {
                let cover = book.cover.clone().unwrap_or_default();
                for page in &book.pages {
                    let recorded_at = page
                        .meta
                        .as_ref()
                        .and_then(|m| m.recorded_at)
                        .unwrap_or_default();
                    let ctx = ProjectionContext::new(cover.clone(), page.sequence, recorded_at);
                    let payload = page.payload.clone().unwrap_or_default();
                    let run = taken_instance.project(&payload, &ctx);
                    if let Some(Err(error)) = match run {
                        Some(run) => Some(run.await),
                        None => None,
                    } {
                        failures.push((entry.name.clone(), error));
                        break 'books;
                    }
                }
            }
            *instance
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(taken_instance);
        }
        failures
    }

    /// One committed `book` through the projector `handler`, named `name`,
    /// its failure by its class (decision 14.37, 14.41): a retryable bug,
    /// and an ABORTED (the kernel being the caller), are run again under
    /// [`OnRetryable`], then dead-lettered with their retry count; any other
    /// (a bug, or a business status, which a projector never has a right
    /// to) is dead-lettered at once. With retrying off, a retryable failure
    /// is returned and not dead-lettered, as a step's is.
    async fn project_book(
        &self,
        name: &str,
        handler: &dyn DynProjectorHandler,
        book: &EventBook,
    ) -> Result<(), Report> {
        let mut retries = 0;
        loop {
            let error = match handler.dispatch(book).await {
                Ok(_) => return Ok(()),
                Err(error) => error,
            };
            let retryable = matches!(
                class(error.grpc as i32),
                Class::BugRetryable | Class::BusinessImmediate
            );
            let error = KernelError::Router(error);
            if !retryable {
                dead_letter::write(
                    self.dead_letters,
                    Payload::RejectedEvents(book.clone()),
                    Component::projector(name),
                    Cause::Kernel(&error),
                );
                return Err(Report::of(&error));
            }
            if self.retry.max_retries == 0 {
                return Err(Report::of(&error));
            }
            if retries >= self.retry.max_retries {
                write_at_cap(
                    self.dead_letters,
                    Payload::RejectedEvents(book.clone()),
                    Component::projector(name),
                    Cause::Kernel(&error),
                    retries,
                );
                return Err(Report::of(&error));
            }
            let backoff = self.retry.backoff(retries, self.wait.draw());
            self.wait.wait(backoff).await;
            retries += 1;
        }
    }

    /// Step 1 for a `D` stream: the book checked, and naming a `D` stream.
    fn check<D: Domain>(&self, book: CommandBook) -> Result<Checked, Failure<Rejected>> {
        let checked =
            check(book, self.registry).map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        if checked.domain() != D::NAME {
            return Err(Failure::Kernel(KernelError::WrongDomain {
                stream: checked.cover().clone(),
                expected: D::NAME,
            }));
        }
        Ok(checked)
    }

    /// Run `projections` over what a committed chain recorded.
    fn project<Ps: Project>(&self, handled: Handled, projections: &mut Ps) -> Handled {
        match handled {
            Handled::Applied(mut recorded) => {
                recorded.projector_failures = projections.project(&recorded, self.registry);
                Handled::Applied(recorded)
            }
            unapplied => unapplied,
        }
    }
}

/// A step's failure, as the chain carries it: a rejection as its report.
fn step_error(failure: Failure<Rejected>) -> StepError {
    match failure {
        Failure::Rejected(rejected) => StepError::Rejected(rejected.report().clone()),
        Failure::Kernel(error) => StepError::Kernel(error),
        Failure::Downstream { error, .. } => error,
        Failure::Projection { .. } => unreachable!("a chain step runs no projector"),
    }
}

/// Write `positions` past a policy step's bug, already dead-lettered,
/// unless `policy_bug` keeps them at the trigger ([`OnPolicyBug::Stay`]).
async fn past_bug<P: Port>(
    policy_bug: OnPolicyBug,
    port: &mut P,
    positions: &[(&str, &Cover, u64)],
) -> Result<(), super::StoreError> {
    match policy_bug {
        OnPolicyBug::MovePast => port.write_positions(positions).await,
        OnPolicyBug::Stay => Ok(()),
    }
}

/// The typed stream a step leaves, for a typed domain: the stream as
/// loaded with `pages` folded on.
fn typed(
    loaded: &super::coordinator::Contextual,
    pages: &[EventPage],
    registry: &Registry,
) -> Result<Option<Box<dyn AnyType + Send>>, KernelError> {
    registry
        .typed_replay(loaded.command.domain())
        .map(|replay| replay(loaded, pages, registry))
        .transpose()
}

/// A chain's failure, as the caller's: the path to the failing step, and
/// why.
fn downstream<R>((path, error): (Vec<Step>, StepError)) -> Failure<R> {
    Failure::Downstream { path, error }
}

/// The path to a failing step, and why it failed.
type Failed = (Vec<Step>, StepError);

/// A chain in progress: the streams whose policies may owe steps, each with
/// the path of steps that led to it, in the order written; what the
/// application sampled for the whole chain (one instant; one minted
/// command id, for the originating command only); and what it recorded.
struct Run<'k> {
    registry: &'k Registry,
    sampled: &'k Sampled,
    dead_letters: &'k dyn DeadLetterLog,
    limit: usize,
    policy_bug: OnPolicyBug,
    /// Mints each delivery's conflict nonce.
    nonces: u64,
    /// The streams to settle, the path to each, and which of its events:
    /// those a step just wrote (`Some(from)`, from that sequence on), or
    /// every one its policies still owe by their positions (`None`).
    queue: VecDeque<(Cover, Vec<Step>, Option<u64>)>,
    recorded: Recorded,
    /// The context the last step's refused command was handed, whole: what
    /// its policy's translating callback reads.
    refused: Option<markup::CommandContext>,
    /// How a step's retryable failure is retried, and the timer.
    retry: OnRetryable,
    wait: &'k dyn Wait,
}

/// The pass-through refusal fact, the kernel's own (decision 14.42), for
/// a refusal its policy does not translate: the standard
/// `CommandRefused { command, refusal, trigger }`, `refusal` the typed
/// refusal object still packed (none for an untyped rejection), `trigger`
/// the triggering event.
fn passed_through(refusal: &markup::v1::Refusal) -> Any {
    let event = refusal.event.as_ref();
    let fact = CommandRefused {
        command: refusal.command.clone(),
        refusal: refusal.response.as_ref().and_then(|r| match &r.reason {
            Some(refusal_response::Reason::Refusal(any)) => Some(any.clone()),
            _ => None,
        }),
        trigger: Some(EventRef {
            stream: event.and_then(|e| e.cover.clone()),
            sequence: event.and_then(|e| e.pages.last()).map_or(0, |p| p.sequence),
        }),
    };
    Any {
        type_url: angzarr_router::type_url(&<CommandRefused as prost::Name>::full_name()),
        value: prost::Message::encode_to_vec(&fact),
    }
}

/// What the originating command's step came to.
enum Origin {
    /// Step 3 found its events already: these.
    Duplicate(EventBook),
    /// A process manager's fact for a stream with no instance, skipped.
    Skipped,
    /// It was decided, and wrote its events (perhaps none) from this
    /// sequence on.
    Wrote(u64),
    /// Its domain is remote: its coordinator's answer.
    Forwarded(Forwarded),
}

/// The class of a step's `failure` ([`class`]), if it
/// is one the kernel may run again: a stale command or a lost race
/// (ABORTED) is the caller's to reload (but a race lost at the write is
/// the kernel's to run again, for any caller, [`raced`]); an unreachable
/// handler or store,
/// and a router error under a retryable status, are retryable bugs; a
/// router error's own class otherwise. `None` for a failure never run
/// again: a refusal, a decide that never acknowledged its window (already
/// dead-lettered), any other.
fn failure_class(failure: &Failure<Rejected>) -> Option<Class> {
    match failure {
        Failure::Kernel(KernelError::Stale { .. })
        | Failure::Kernel(KernelError::Overlap { .. })
        | Failure::Kernel(KernelError::Store(super::StoreError::Conflict { .. })) => {
            Some(Class::BusinessImmediate)
        }
        Failure::Kernel(KernelError::HandlerUnavailable { .. })
        | Failure::Kernel(KernelError::Upcast {
            error: super::UpcastError::Unavailable { .. },
            ..
        }) => Some(Class::BugRetryable),
        Failure::Kernel(KernelError::Store(error))
            if super::error::store_recovery(error) == super::Recovery::Retry =>
        {
            Some(Class::BugRetryable)
        }
        Failure::Kernel(KernelError::Router(error)) => Some(class(error.grpc as i32)),
        _ => None,
    }
}

/// The store's refusal `error` of the write of `contextual`'s events, in
/// the kernel's terms. A write lost to another write to the same stream
/// (the store's conflict: it landed between this command's load and its
/// write) under STRICT, by a command whose sender set its expected
/// sequence, is that sequence gone stale: [`KernelError::Stale`], against
/// the head the store found (decision 14.45). Any other error, any other
/// lost race included ([`raced`]), is the store's.
fn lost(contextual: &Contextual, error: StoreError) -> KernelError {
    match (&error, contextual.command.expected_sequence()) {
        (StoreError::Conflict { actual, .. }, Some(expected))
            if contextual.resolution == ConflictResolution::Strict =>
        {
            KernelError::Stale {
                stream: contextual.command.cover().clone(),
                expected,
                actual: *actual,
            }
        }
        _ => KernelError::Store(error),
    }
}

/// How a race lost at the write is run again (decisions 14.45, 14.46).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Race {
    /// The kernel stamped the head it loaded (no expected sequence, or a
    /// fact): decided again on the new head, under the race re-run cap.
    Rerun,
    /// The sender's expected sequence, under AGGREGATE_HANDLES or
    /// COMMUTATIVE: the widened window re-delivered with a fresh nonce,
    /// the resolution's rule deciding again, under the retry cap.
    Redeliver,
}

/// Whether `failure` is a write `command` lost to another write to its
/// stream, and how it is run again, for any caller (optimistic
/// concurrency, per stream only). STRICT's loss with an expected sequence
/// is never one: [`lost`] made it [`KernelError::Stale`].
fn raced(failure: &Failure<Rejected>, command: &Identified) -> Option<Race> {
    match failure {
        Failure::Kernel(KernelError::Store(StoreError::Conflict { .. })) => {
            Some(match command.expected_sequence() {
                None => Race::Rerun,
                Some(_) => Race::Redeliver,
            })
        }
        _ => None,
    }
}

/// How often one command's step has been run again: retries of a
/// retryable failure, and runs that lost the race at the write.
#[derive(Default)]
struct Tries {
    retries: u32,
    races: u32,
}

/// Whether `failure` is a step given up at a cap, dead-lettered already:
/// its policy's position does what a bug's does.
fn given_up(failure: &Failure<Rejected>) -> bool {
    matches!(
        failure,
        Failure::Kernel(KernelError::RetryCapExhausted { .. })
    )
}

/// What a step came to.
enum Stepped {
    /// It wrote events to its stream, from this sequence on.
    Wrote(Cover, u64),
    /// It wrote no event: a duplicate, or a decision with none.
    Nothing,
}

impl<'k> Run<'k> {
    fn new(kernel: &Kernel<'k>, sampled: &'k Sampled) -> Run<'k> {
        Run {
            registry: kernel.registry,
            sampled,
            dead_letters: kernel.dead_letters,
            limit: kernel.limit,
            policy_bug: kernel.policy_bug,
            nonces: 0,
            queue: VecDeque::new(),
            recorded: Recorded::default(),
            refused: None,
            retry: kernel.retry,
            wait: kernel.wait,
        }
    }

    /// Whether a step's `failure` is run again: retrying is on, and it is a
    /// retryable bug, or, as a `policy_step` (whose caller is the kernel),
    /// ABORTED. A race lost at the write (the store's conflict) is never
    /// retried here: [`Run::again`] re-runs it to its race cap, dead-lettered
    /// there and given up ([`given_up`]); handed back at a race re-run cap
    /// of 0, it is neither, and a policy step's position holds.
    fn retried(&self, failure: &Failure<Rejected>, policy_step: bool) -> bool {
        if matches!(
            failure,
            Failure::Kernel(KernelError::Store(StoreError::Conflict { .. }))
        ) {
            return false;
        }
        self.retry.max_retries > 0
            && match failure_class(failure) {
                Some(Class::BugRetryable) => true,
                Some(Class::BusinessImmediate) => policy_step,
                _ => false,
            }
    }

    /// Whether `command`'s step, failed with `failure` after `tries`, runs
    /// again (`Ok(true)`): once its backoff is waited out, `tries` one
    /// more. A failure that is retried but has had all its retries is not
    /// (`Ok(false)`). A race lost at the write ([`raced`]) runs again up to
    /// its cap: the race re-run cap for a stamped command, `max_retries +
    /// 1` deliveries for a re-delivered window; at the cap it is given up,
    /// [`KernelError::RetryCapExhausted`] in its place (`Err`). A race
    /// re-run cap of 0 runs a stamped command's lost race no more
    /// (`Ok(false)`): its caller is handed the conflict.
    ///
    /// At either cap, what has no caller left is dead-lettered, with its
    /// retries: a `policy_step`, and a fact, which is parked. A command its
    /// caller is waiting for is not: the caller is answered, and holds it
    /// (owner ruling, 2026-10-31).
    async fn again(
        &mut self,
        failure: &Failure<Rejected>,
        policy_step: bool,
        tries: &mut Tries,
        command: &Identified,
    ) -> Result<bool, Failure<Rejected>> {
        if let Some(race) = raced(failure, command) {
            if matches!(race, Race::Rerun) && self.retry.max_race_runs == 0 {
                return Ok(false);
            }
            tries.races += 1;
            let cap = match race {
                Race::Rerun => self.retry.max_race_runs,
                Race::Redeliver => self.retry.max_retries.saturating_add(1),
            };
            if tries.races >= cap {
                let given_up = KernelError::RetryCapExhausted {
                    stream: command.cover().clone(),
                    attempts: tries.races,
                    cap,
                };
                if Self::callerless(policy_step, command) {
                    dead_letter::write(
                        self.dead_letters,
                        Payload::RejectedCommand(command.to_book()),
                        Component::domain(self.registry, command.domain()),
                        Cause::Kernel(&given_up),
                    );
                }
                return Err(Failure::Kernel(given_up));
            }
            let backoff = self.retry.backoff(tries.races - 1, self.wait.draw());
            self.wait.wait(backoff).await;
            return Ok(true);
        }
        if !self.retried(failure, policy_step) {
            return Ok(false);
        }
        if tries.retries >= self.retry.max_retries {
            // Only a kernel error is retried ([`failure_class`]).
            if let (true, Failure::Kernel(error)) =
                (Self::callerless(policy_step, command), failure)
            {
                write_at_cap(
                    self.dead_letters,
                    Payload::RejectedCommand(command.to_book()),
                    Component::domain(self.registry, command.domain()),
                    Cause::Kernel(error),
                    tries.retries,
                );
            }
            return Ok(false);
        }
        let backoff = self.retry.backoff(tries.retries, self.wait.draw());
        self.wait.wait(backoff).await;
        tries.retries += 1;
        Ok(true)
    }

    /// Whether `command`, given up on at a cap, has no caller left to hold
    /// it: a `policy_step`'s (whose caller is the kernel), or a fact's,
    /// parked. A command its caller waits for is the caller's.
    fn callerless(policy_step: bool, command: &Identified) -> bool {
        policy_step || command.body().is_fact()
    }

    /// The originating command's step: load then deduplicate, then
    /// dispatch through the router and write its events, alone.
    async fn origin<P: Port>(
        &mut self,
        port: &mut P,
        identified: Identified,
    ) -> Result<Origin, Failure<Rejected>> {
        if let Some(Target::Remote(remote)) = self.registry.target(identified.domain()) {
            let forwarded = forward(remote, &identified, self.dead_letters).await?;
            return Ok(Origin::Forwarded(forwarded));
        }
        let cover = identified.cover().clone();
        let novel = match deduplicate(port, self.registry, identified).await? {
            Deduplicated::Duplicate { events } => return Ok(Origin::Duplicate(events)),
            Deduplicated::Skipped => return Ok(Origin::Skipped),
            Deduplicated::Novel(novel) => novel,
        };
        let contextual = load(*novel, &mut self.nonces).map_err(Failure::Kernel)?;
        let head = contextual.head();
        let decided = decide(&contextual, self.sampled, self.registry, self.dead_letters).await?;
        let pages = decided.pages;
        let stream = typed(&contextual, &pages, self.registry).map_err(Failure::Kernel)?;
        if !pages.is_empty() {
            port.write(&cover, head, &pages, &[])
                .await
                .map_err(|e| Failure::Kernel(lost(&contextual, e)))?;
            write_snapshot(port, &cover, decided.snapshot).await;
        }
        self.record(cover, head, stream, pages);
        Ok(Origin::Wrote(head))
    }

    /// Settle every queued stream, in order: each runs its policies on
    /// every event they have yet to handle; a stream a step writes to is
    /// queued in its turn.
    async fn settle<P: Port>(&mut self, port: &mut P) -> Result<(), Failed> {
        while let Some((source, path, written)) = self.queue.pop_front() {
            self.settle_stream(port, &source, &path, written).await?;
        }
        Ok(())
    }

    /// Run `source`'s policies on its events, each event in order, each
    /// policy in registration order: on the events a step just wrote, from
    /// `written` on; or, `written` `None`, on every event from each
    /// policy's position on. A policy whose position is past an event has
    /// handled it.
    async fn settle_stream<P: Port>(
        &mut self,
        port: &mut P,
        source: &Cover,
        path: &[Step],
        written: Option<u64>,
    ) -> Result<(), Failed> {
        let registry = self.registry;
        let policies: Vec<_> = registry.policies(&source.domain).collect();
        if policies.is_empty() {
            return Ok(());
        }
        let store = |e| (path.to_vec(), StepError::Kernel(KernelError::Store(e)));
        let mut positions = Vec::with_capacity(policies.len());
        for policy in &policies {
            positions.push(port.position(policy.name(), source).await.map_err(store)?);
        }
        let owed = positions.iter().copied().min().unwrap_or_default();
        let from = written.map_or(owed, |written| written.max(owed));
        let slice = port.read_from(source, from).await.map_err(store)?;
        let sidecar = registry.upcast_port(&source.domain);
        for page in &slice.pages {
            let event = match sidecar {
                Some(sidecar) => upcast_pages(sidecar, &source.domain, std::slice::from_ref(page))
                    .await
                    .map(|mut upcast| upcast.pop().unwrap_or_else(|| page.clone()))
                    .map_err(|error| {
                        let error = KernelError::Upcast {
                            stream: source.clone(),
                            error,
                        };
                        (path.to_vec(), StepError::Kernel(error))
                    })?,
                None => page.clone(),
            };
            for (k, policy) in policies.iter().enumerate() {
                if page.sequence < positions[k] {
                    continue;
                }
                self.run_policy(port, policy, source, &event, path).await?;
            }
        }
        Ok(())
    }

    /// One policy on one event of `source` (`page`, upcast), through its
    /// angzarr-router's `PolicyComponent`: each command it answers a step of
    /// its own, its position written with the last (or alone, when no step
    /// writes it). The kernel's refusal of the answer ([`check_reactions`]:
    /// a command outside the policy's destination or its trigger's edition,
    /// with no root, with Handling, a Fact, carrying `fact_origin`, in
    /// another correlation, or naming an identity other than its stamp) is
    /// the policy's bug, as is the
    /// router's own error: dead-lettered, and the position stays. The policy's own error ([`StepError::PolicyFailed`]) and a
    /// handler's bug in one of its steps are dead-lettered by their code
    /// and args, and the position moves past the event (or stays, under
    /// [`OnPolicyBug::Stay`]).
    async fn run_policy<P: Port>(
        &mut self,
        port: &mut P,
        policy: &PolicyEntry,
        source: &Cover,
        page: &EventPage,
        path: &[Step],
    ) -> Result<(), Failed> {
        let next = page.sequence + 1;
        let position = [(policy.name(), source, next)];
        let trigger = EventRef {
            stream: Some(source.clone()),
            sequence: page.sequence,
        };
        let event = EventBook {
            cover: Some(source.clone()),
            pages: vec![page.clone()],
            ..EventBook::default()
        };
        let checked = policy
            .dispatch
            .dispatch(&event)
            .await
            .and_then(|reactions| {
                check_reactions(
                    &policy.output_domain,
                    &event,
                    reactions.reactions,
                    policy.name(),
                    &trigger,
                )
            });
        let commands = match checked {
            Ok(commands) => commands,
            Err(error) => {
                if let Some(failure) = super::route::policy_failure(&error) {
                    dead_letter::write(
                        self.dead_letters,
                        Payload::RejectedEvents(event),
                        Component::policy(policy.name()),
                        Cause::PolicyFailed(&failure),
                    );
                    past_bug(self.policy_bug, port, &position)
                        .await
                        .map_err(|e| (path.to_vec(), StepError::Kernel(KernelError::Store(e))))?;
                    return Err((path.to_vec(), StepError::PolicyFailed(failure)));
                }
                let error = KernelError::Router(error);
                dead_letter::write(
                    self.dead_letters,
                    Payload::RejectedEvents(event),
                    Component::policy(policy.name()),
                    Cause::Kernel(&error),
                );
                return Err((path.to_vec(), StepError::Kernel(error)));
            }
        };
        let correlation = page
            .meta
            .as_ref()
            .and_then(|m| m.correlation_id.clone())
            .unwrap_or_default();
        let orders = answered(policy.name(), &trigger, &correlation, commands);
        let store = |path: Vec<Step>, e| (path, StepError::Kernel(KernelError::Store(e)));
        if orders.is_empty() {
            return port
                .write_positions(&position)
                .await
                .map_err(|e| store(path.to_vec(), e));
        }
        let last = orders.len() - 1;
        for (index, order) in orders.iter().enumerate() {
            let mut path = path.to_vec();
            path.push(Step {
                stream: order.cover().clone(),
                cause: order.trigger().clone(),
            });
            if path.len() > self.limit {
                let limit = self.limit;
                return Err((path, StepError::Kernel(KernelError::ChainTooDeep { limit })));
            }
            let checked = match order.check_logged(self.registry, self.dead_letters) {
                Ok(checked) => checked,
                Err(m) => return Err((path, StepError::Kernel(KernelError::Malformed(m)))),
            };
            let delivered = checked.identify(self.sampled);
            let positions: &[(&str, &Cover, u64)] = match index == last {
                true => &position,
                false => &[],
            };
            match self.step(port, delivered.clone(), positions).await {
                Ok(Stepped::Wrote(cover, head)) => self.queue.push_back((cover, path, Some(head))),
                Ok(Stepped::Nothing) => {}
                Err(Failure::Rejected(rejected)) => {
                    if !policy.answers_refusals {
                        dead_letter::write(
                            self.dead_letters,
                            Payload::RejectedCommand(delivered.to_book()),
                            Component::policy(policy.name()),
                            Cause::Refused(&rejected),
                        );
                        port.write_positions(&position)
                            .await
                            .map_err(|e| store(path.clone(), e))?;
                        return Err((path, StepError::Rejected(rejected.report().clone())));
                    }
                    let refusal = markup::v1::Refusal {
                        event: Some(event.clone()),
                        command: Some(delivered.to_book()),
                        response: Some(rejected.response()),
                    };
                    let ctx = self.refused.take().unwrap_or_default();
                    match self
                        .refusal_step(port, policy, &refusal, &ctx, &delivered, positions)
                        .await
                    {
                        Ok(Stepped::Wrote(cover, head)) => {
                            self.queue.push_back((cover, path, Some(head)))
                        }
                        Ok(Stepped::Nothing) => {}
                        Err(error) => return Err((path, error)),
                    }
                }
                Err(Failure::Kernel(KernelError::Router(error)))
                    if super::route::is_handler_fault(&error) =>
                {
                    past_bug(self.policy_bug, port, &position)
                        .await
                        .map_err(|e| store(path.clone(), e))?;
                    return Err((path, StepError::Kernel(KernelError::Router(error))));
                }
                Err(failure) => {
                    // Retried or re-run to its cap and dead-lettered: the
                    // position does what a bug's does.
                    if self.retried(&failure, true) || given_up(&failure) {
                        past_bug(self.policy_bug, port, &position)
                            .await
                            .map_err(|e| store(path.clone(), e))?;
                    }
                    return Err((path, step_error(failure)));
                }
            }
        }
        Ok(())
    }

    /// `delivered`'s refusal fact, the policy's OnRefused answer to
    /// `refusal` (its translating callback handed `ctx`, the context
    /// `delivered` was handed at its target), or with no translation the
    /// pass-through `CommandRefused` the kernel builds, addressed by the
    /// kernel (decision 14.42), as a
    /// step of its own in the stream of the triggering event, with the
    /// refused command's id and correlation. A refusal its source has no handler for is
    /// dead-lettered, and writes only `positions`; one whose handler fails
    /// is the policy's failure ([`StepError::PolicyFailed`]), dead-lettered
    /// by its code and args, and one its source's handler fails on with a
    /// bug is dead-lettered as that bug: both write only `positions`, unless
    /// the kernel says to stay ([`OnPolicyBug::Stay`]).
    async fn refusal_step<P: Port>(
        &mut self,
        port: &mut P,
        policy: &PolicyEntry,
        refusal: &markup::v1::Refusal,
        ctx: &markup::CommandContext,
        delivered: &Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, StepError> {
        let kernel = |error| StepError::Kernel(KernelError::Router(error));
        let answer = match policy.dispatch.dispatch_refusal(refusal, ctx).await {
            Ok(answer) => answer,
            Err(error) => {
                let Some(failure) = super::route::policy_failure(&error) else {
                    return Err(kernel(error));
                };
                dead_letter::write(
                    self.dead_letters,
                    Payload::RejectedCommand(delivered.to_book()),
                    Component::policy(policy.name()),
                    Cause::PolicyFailed(&failure),
                );
                past_bug(self.policy_bug, port, positions)
                    .await
                    .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
                return Err(StepError::PolicyFailed(failure));
            }
        };
        let fact = answer.unwrap_or_else(|| passed_through(refusal));
        let trigger = delivered.causation_id().cloned().unwrap_or_default();
        let book = CommandBook {
            // The exact stream of the triggering event.
            cover: refusal.event.as_ref().and_then(|e| e.cover.clone()),
            page: Some(CommandPage {
                body: Some(command_page::Body::Fact(Fact {
                    payload: Some(fact),
                })),
                fact_origin: Some(FactOrigin {
                    external_id: delivered.command_id().to_string(),
                    description: format!(
                        "{} refused by {}",
                        type_name(&delivered.payload().type_url),
                        delivered.domain()
                    ),
                }),
                ..CommandPage::default()
            }),
            handling: None,
        };
        let checked = check_refusal(
            book.clone(),
            self.registry,
            trigger,
            delivered.correlation_id().clone(),
            delivered.command_id().clone(),
        )
        .map_err(|m| {
            dead_letter::write(
                self.dead_letters,
                Payload::RejectedCommand(book),
                Component::policy(policy.name()),
                Cause::Malformed(&m),
            );
            StepError::Kernel(KernelError::Malformed(m))
        })?;
        let fact = checked.identify(self.sampled);
        let error = match self.step(port, fact.clone(), positions).await {
            Ok(stepped) => return Ok(stepped),
            Err(Failure::Kernel(KernelError::Router(error)))
                if !matches!(
                    class(error.grpc as i32),
                    Class::BusinessImmediate | Class::BugRetryable
                ) =>
            {
                error
            }
            Err(failure) => {
                if self.retried(&failure, true) || given_up(&failure) {
                    past_bug(self.policy_bug, port, positions)
                        .await
                        .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
                }
                return Err(step_error(failure));
            }
        };
        if let Some(failure) = super::route::policy_failure(&error) {
            dead_letter::write(
                self.dead_letters,
                Payload::RejectedCommand(fact.to_book()),
                Component::domain(self.registry, fact.domain()),
                Cause::PolicyFailed(&failure),
            );
            past_bug(self.policy_bug, port, positions)
                .await
                .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
            return Err(StepError::PolicyFailed(failure));
        }
        if error.code != RouterReason::NoFactHandler.as_str_name()
            && error.code != RouterReason::NoRefusalHandler.as_str_name()
        {
            if super::route::is_handler_fault(&error) {
                past_bug(self.policy_bug, port, positions)
                    .await
                    .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
            }
            return Err(StepError::Kernel(KernelError::Router(error)));
        }
        // The router answers a refusal with no path INTERNAL, a bug: the
        // dispatch dead-lettered it already.
        port.write_positions(positions)
            .await
            .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
        Ok(Stepped::Nothing)
    }

    /// `command` as a step of its own, as [`Run::attempt`] runs it, run
    /// again while its failure is retried ([`Run::again`]): a policy's
    /// step, whose caller is the kernel.
    async fn step<P: Port>(
        &mut self,
        port: &mut P,
        command: Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, Failure<Rejected>> {
        let mut tries = Tries::default();
        loop {
            let failure = match self.attempt(port, command.clone(), positions).await {
                Err(failure) => failure,
                stepped => return stepped,
            };
            match self.again(&failure, true, &mut tries, &command).await {
                Ok(true) => {}
                Ok(false) => return Err(failure),
                Err(given_up) => return Err(given_up),
            }
        }
    }

    /// `command` as a step of its own in the stream it names: load,
    /// deduplicate, dispatch through the router, and write its events with
    /// `positions` (or `positions` alone, when it writes no event).
    async fn attempt<P: Port>(
        &mut self,
        port: &mut P,
        command: Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, Failure<Rejected>> {
        let kernel = Failure::Kernel;
        let store = |e| kernel(KernelError::Store(e));
        if let Some(Target::Remote(remote)) = self.registry.target(command.domain()) {
            self.refused = None;
            let forwarded = forward(remote, &command, self.dead_letters).await?;
            if !positions.is_empty() {
                port.write_positions(positions).await.map_err(store)?;
            }
            if let Forwarded::Applied(events) = forwarded {
                self.answered(events);
            }
            return Ok(Stepped::Nothing);
        }
        let cover = command.cover().clone();
        let novel = match deduplicate(port, self.registry, command).await? {
            Deduplicated::Duplicate { .. } | Deduplicated::Skipped => {
                if !positions.is_empty() {
                    port.write_positions(positions).await.map_err(store)?;
                }
                return Ok(Stepped::Nothing);
            }
            Deduplicated::Novel(novel) => novel,
        };
        let contextual = load(*novel, &mut self.nonces).map_err(kernel)?;
        let head = contextual.head();
        let (pages, snapshot) =
            match decide(&contextual, self.sampled, self.registry, self.dead_letters).await {
                Ok(decided) => (decided.pages, decided.snapshot),
                Err(failure) => {
                    if let Failure::Rejected(_) = failure {
                        self.refused =
                            contextual_command(&contextual, self.registry, self.sampled.now)
                                .await
                                .ok()
                                .map(|delivered| {
                                    angzarr_router::context::command_context(&delivered)
                                });
                    }
                    return Err(failure);
                }
            };
        if pages.is_empty() {
            if !positions.is_empty() {
                port.write_positions(positions).await.map_err(store)?;
            }
            return Ok(Stepped::Nothing);
        }
        let stream = typed(&contextual, &pages, self.registry).map_err(kernel)?;
        port.write(&cover, head, &pages, positions)
            .await
            .map_err(|e| kernel(lost(&contextual, e)))?;
        write_snapshot(port, &cover, snapshot).await;
        self.record(cover.clone(), head, stream, pages);
        Ok(Stepped::Wrote(cover, head))
    }

    /// A remote coordinator's `events` for a step it handled, as the
    /// chain's answer: in its books, beside the streams written here, and
    /// not among the steps written here (nothing of it was).
    fn answered(&mut self, events: EventBook) {
        if !events.pages.is_empty() {
            self.recorded.books.push(events);
        }
    }

    /// A step's write: its events, as the chain's answer.
    fn record(
        &mut self,
        cover: Cover,
        head: u64,
        stream: Option<Box<dyn AnyType + Send>>,
        pages: Vec<EventPage>,
    ) {
        if pages.is_empty() {
            return;
        }
        let next_sequence = head + pages.len() as u64;
        self.recorded.steps.push(EventBook {
            cover: Some(cover.clone()),
            snapshot: None,
            next_sequence,
            pages: pages.clone(),
        });
        match self
            .recorded
            .books
            .iter_mut()
            .find(|book| book.cover.as_ref() == Some(&cover))
        {
            Some(book) => {
                book.pages.extend(pages);
                book.next_sequence = next_sequence;
            }
            None => self.recorded.books.push(EventBook {
                cover: Some(cover.clone()),
                snapshot: None,
                next_sequence,
                pages,
            }),
        }
        let Some(stream) = stream else {
            return;
        };
        match self.recorded.streams.iter_mut().find(|(c, ..)| *c == cover) {
            Some((_, _, held)) => *held = stream,
            None => self.recorded.streams.push((cover, head, stream)),
        }
    }
}

/// Everything a chain recorded, every stream, after commit: angzarr's
/// `CommandResponse.events`, one `EventBook` per stream here, since a chain
/// touches several.
#[derive(Default)]
pub struct Recorded {
    /// One book per stream touched, in the order first touched: the pages
    /// appended, `next_sequence` the new length.
    books: Vec<EventBook>,
    /// The streams after the chain (each a `Stream<D>`, boxed), with the
    /// length each was loaded at.
    streams: Vec<(Cover, u64, Box<dyn AnyType + Send>)>,
    /// Projections that failed after the commit.
    projector_failures: Vec<ProjectorFailure>,
    /// Each step's events, one book per step that wrote any, in the order
    /// written.
    steps: Vec<EventBook>,
}

impl Recorded {
    /// Each committed step's events, one book per step (a step that wrote
    /// no event is not listed), in the order written: the command's first,
    /// then each policy step's and refusal fact's.
    pub fn steps(&self) -> &[EventBook] {
        &self.steps
    }

    /// What the chain appended, as `EventBook`s: every page with its
    /// `EventMeta`.
    pub fn books(&self) -> &[EventBook] {
        &self.books
    }

    /// The events the chain recorded in `stream`, if it is a `D` stream the
    /// chain touched.
    pub fn events<D: Domain>(&self, stream: &Cover) -> Option<&[Envelope<D::Event>]> {
        let (_, loaded_at, boxed) = self.streams.iter().find(|(c, ..)| c == stream)?;
        let stream = boxed.downcast_ref::<Stream<D>>()?;
        Some(&stream.events()[(loaded_at - stream.first()) as usize..])
    }

    /// Hand the chain's events in `stream` to `handlers`, each with the
    /// state just after it. Nothing, for a stream the chain did not touch;
    /// [`KernelError::WrongDomain`] for one that is not a `D` stream.
    pub fn dispatch<D: Domain>(
        &self,
        stream: &Cover,
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        let Some((_, loaded_at, boxed)) = self.streams.iter().find(|(c, ..)| c == stream) else {
            return Ok(());
        };
        let recorded =
            boxed
                .downcast_ref::<Stream<D>>()
                .ok_or_else(|| KernelError::WrongDomain {
                    stream: stream.clone(),
                    expected: D::NAME,
                })?;
        recorded.dispatch(*loaded_at, registry, handlers)
    }

    /// The projections that failed on the chain's events (angzarr's
    /// `Outcome.projector_failures`). The commit stands regardless.
    pub fn projector_failures(&self) -> &[ProjectorFailure] {
        &self.projector_failures
    }
}

/// One projection that failed after the commit.
#[derive(Clone, PartialEq, Debug)]
pub struct ProjectorFailure {
    /// The [`ProjectionRoute::name`].
    pub projector: &'static str,
    pub error: KernelError,
}

/// Projections the command handler runs after commit, over what the chain
/// recorded: [`ProjectionRoute`]s; `()` is none, a pair is both. Each
/// returns the projections that failed; a failure never stops the others.
pub trait Project {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure>;
}

/// A projection of one domain's streams, run after commit over every
/// stream of that domain the chain recorded in.
pub struct ProjectionRoute<'p, D: Domain, P: Projection<D>> {
    /// Named in a [`ProjectorFailure`].
    pub name: &'static str,
    pub projection: &'p mut P,
    pub domain: std::marker::PhantomData<fn() -> D>,
}

impl Project for () {
    fn project(&mut self, _recorded: &Recorded, _registry: &Registry) -> Vec<ProjectorFailure> {
        Vec::new()
    }
}

impl<A: Project, B: Project> Project for (A, B) {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure> {
        let mut failures = self.0.project(recorded, registry);
        failures.extend(self.1.project(recorded, registry));
        failures
    }
}

impl<D: Domain, P: Projection<D>> Project for ProjectionRoute<'_, D, P> {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure> {
        let mut failures = Vec::new();
        for (cover, ..) in &recorded.streams {
            if cover.domain != D::NAME {
                continue;
            }
            let mut handler = projection(&mut *self.projection);
            if let Err(error) = recorded.dispatch::<D>(cover, registry, &mut [&mut handler]) {
                failures.push(ProjectorFailure {
                    projector: self.name,
                    error,
                });
            }
        }
        failures
    }
}

#[cfg(test)]
#[path = "handle.test.rs"]
mod tests;

// Same-stream concurrency, optimistic: forced races on one stream.
#[cfg(test)]
#[path = "concurrency.test.rs"]
mod concurrency_tests;

// The conflict-resolution contract at load and at the write, and the caps.
#[cfg(test)]
#[path = "races.test.rs"]
mod races_tests;

/// A future that is pending once, waking itself: a turn for another task.
struct Yield(bool);

impl std::future::Future for Yield {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

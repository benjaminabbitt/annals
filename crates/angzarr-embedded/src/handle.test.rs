//! The chain as a sequence of committed steps: atomicity covers one
//! handling step, never a chain. A command commits on its own stream; each
//! policy step after it is one atomic write to one stream, committed with
//! the policy's position on its source; a step that fails keeps every commit before it, and a step left
//! undone (a crash, a failed store) is resumed by position, deduplicated by
//! its derived command id. The call waits for the chain to settle.

use crate::recording::Recording;
use crate::toys::prelude::*;
use crate::v1::event_meta::Causation;
use crate::v1::{CommandBook, EventMeta, EventRef};
use crate::{
    ids, Failure, Kernel, KernelError, MemoryDeadLetters, OnPolicyBug, Payload, Recovery, Step,
    StepError,
};

use crate::toys::{
    at, command, cover, cover_in, ledger, ledger_root, opened, payloads, registry, Account,
    AccountCmd, Bookkeeping, Edit, Ledger, LEDGER_LIMIT,
};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn ledger_cover() -> crate::v1::Cover {
    crate::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: None,
    }
}

#[test]
fn a_policys_events_carry_the_derived_command_id_as_causation_and_no_trigger() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);

    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25).correlated(8), &at(30), &mut ())
        .ready()
        .unwrap();
    let books = handled.books();
    assert_eq!(books.len(), 2, "the account, then the ledger");
    assert_eq!(books[1].cover, Some(ledger_cover()));
    assert_eq!(
        payloads::<ledger::Recorded>(&books[1..]),
        [ledger::Recorded {
            source_sequence: 2,
            cents: 25
        }]
    );

    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    let meta: EventMeta = books[1].pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(Causation::CommandId(
            ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap()
        )),
        "the direct cause is the command, UUIDv5 of (trigger, component, \
         index) as angzarr derives it; never the trigger (decision 13.3)"
    );
    assert_eq!(
        meta.correlation_id,
        Some(crate::toys::correlation(8)),
        "the trigger's"
    );
    assert_eq!(
        meta.recorded_at.unwrap().seconds,
        30,
        "one instant per chain"
    );
}

/// A policy step its target rejects fails the call with the path to the
/// step, but rolls nothing back: the command's commit stands. The rejection
/// is a business answer, so the step is dead-lettered and its position moves
/// past the trigger, and nothing runs it again: a retry of the command is a
/// duplicate, and owes nothing.
#[test]
fn a_rejected_policy_step_keeps_the_commands_commit_and_is_dead_lettered() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;

    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream { path, error } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    assert_eq!(
        path,
        &[Step {
            stream: ledger_cover(),
            cause: EventRef {
                stream: Some(cover("account", 1)),
                sequence: 2,
            },
        }]
    );
    assert!(
        matches!(error, StepError::Rejected(r) if angzarr_diagnostic::Diagnostic::code(r) == "toy-ledger-too-large")
    );
    assert_eq!(failure.recovery(), Recovery::Business);
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the command's deposit committed, and stays"
    );
    assert_eq!(
        pages(&store, &ledger_cover()),
        [],
        "the step landed nothing"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "the rejected step, dead-lettered");
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    // An untyped business refusal: the handler's own key, and
    // processing_failed, never retried, not transient (decision 14.48).
    let letter = crate::toys::one_letter(log.letters());
    let reason = crate::toys::wire("toy-ledger-too-large", &[]);
    crate::toys::assert_letter(
        &letter,
        crate::v1::ComponentKind::Policy,
        &reason,
        crate::toys::processing_failed(&reason, 0, false),
    );
    // The command as delivered, its ids assigned.
    let Some(crate::v1::angzarr_dead_letter::Payload::RejectedCommand(book)) = &letter.payload
    else {
        panic!("the refused command: {letter:?}");
    };
    let page = book.page.as_ref().unwrap();
    assert!(page.command_id.is_some(), "its id: {book:?}");
    assert!(page.correlation_id.is_some(), "its correlation: {book:?}");
    assert!(page.causation_id.is_some(), "its trigger: {book:?}");
    assert_eq!(book.cover.as_ref(), Some(&ledger_cover()));
    assert_eq!(letter.source_component, "bookkeeping");
    assert_eq!(
        position(&store, "bookkeeping", &cover("account", 1)),
        before + 1,
        "past the rejected trigger"
    );

    let retried = kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(31), &mut ())
        .ready()
        .unwrap();
    assert!(matches!(retried, crate::Handled::Duplicate(_)));
    assert_eq!(pages(&store, &ledger_cover()), [], "nothing run again");
    assert_eq!(log.lines().len(), 1);
}

#[test]
fn a_chain_deeper_than_its_limit_is_refused() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).limit(0);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, 1), &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    assert!(matches!(
        failure,
        Failure::Downstream {
            error: StepError::Kernel(KernelError::ChainTooDeep { limit: 0 }),
            ..
        }
    ));
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        pages(&store, &cover("account", 1)).len(),
        3,
        "the command's commit stands"
    );
}

/// Editions are per-stream forks (14.32): a command to a stream not forked
/// into its edition is outside input to it, refused by the store at the
/// write (`EDITION_NOT_FORKED`), and nothing is written. angzarr-embedded creates no
/// edition yet.
#[test]
fn a_command_to_an_edition_is_refused_and_writes_nothing() {
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let stream = cover_in("account", 1, "what-if");
    let open = command(stream.clone(), AccountCmd::Open.to_any());
    let failure = kernel
        .handle::<Account, _, _>(&store, open, &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Store(crate::StoreError::EditionNotForked {
            stream: Box::new(stream)
        }))
    );
    assert_eq!(failure.recovery(), Recovery::Invalid);
    assert_eq!(streams(&store), [], "nothing written");
}

#[test]
fn a_policys_target_is_in_its_sources_edition() {
    let target =
        crate::target_cover::<Bookkeeping>(&cover_in("account", 1, "what-if"), &ledger_root());
    assert_eq!(
        target.edition.as_deref(),
        Some("what-if"),
        "a policy never crosses timelines"
    );
}

/// A hand-written policy's refusal answer is angzarr's pass-through
/// `CommandRefused` unless it says otherwise.
#[test]
fn a_hand_written_policy_passes_a_refusal_through() {
    use crate::markup::{self, v1::Refusal};
    let refusal = Refusal {
        event: Some(crate::v1::EventBook {
            cover: Some(cover("account", 1)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let answer = crate::Policy::on_refused(&Bookkeeping, &refusal, &Default::default());
    assert_eq!(answer, Ok(markup::pass_through(&refusal)));
}

#[test]
fn a_book_for_another_domain_is_refused() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let failure = kernel
        .handle::<Ledger, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        })
    );
}

/// Each step loads what every step before it committed: the second
/// deposit's ledger step finds the first's record. What a chain answers is
/// every stream it committed to, in the order first touched.
#[test]
fn each_step_loads_what_the_steps_before_it_committed() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .unwrap();
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(4, 5), &at(31), &mut ())
        .ready()
        .unwrap();
    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert_eq!(
        recorded
            .books()
            .iter()
            .map(|b| (b.cover.clone().unwrap(), b.pages.len(), b.next_sequence))
            .collect::<Vec<_>>(),
        [(cover("account", 1), 1, 4), (ledger_cover(), 1, 2)],
        "each stream once, in the order first touched"
    );
    let ledger = recorded.events::<Ledger>(&ledger_cover()).unwrap();
    assert_eq!(ledger[0].sequence(), 1, "after the first deposit's record");
    assert!(recorded.events::<Ledger>(&cover("account", 1)).is_none());
    assert!(recorded.events::<Account>(&cover("account", 9)).is_none());
}

/// Every deposit's amount, projected after commit.
#[derive(Default)]
struct Deposits(Vec<i64>);

impl crate::Projection<Account> for Deposits {
    fn project(&mut self, c: &crate::Committed<'_, Account>) {
        if let crate::toys::AccountEvent::Deposited(cents) = c.event() {
            self.0.push(*cents);
        }
    }
}

/// Projections run on what each step committed: the command's events and
/// every policy step's.
#[test]
fn projections_run_after_commit_over_their_domains_streams() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let mut deposits = Deposits::default();
    let mut ledger_seen = 0;
    let mut route = crate::ProjectionRoute {
        name: "deposits",
        projection: &mut deposits,
        domain: std::marker::PhantomData,
    };
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut route)
        .ready()
        .unwrap();
    assert!(handled.outcome().projector_failures.is_empty());
    assert_eq!(deposits.0, [25], "only the new events, only the account's");

    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    recorded
        .dispatch::<Ledger>(
            &ledger_cover(),
            &registry,
            &mut [&mut |_: &crate::Committed<'_, Ledger>| ledger_seen += 1],
        )
        .unwrap();
    assert_eq!(ledger_seen, 1);
    assert_eq!(
        recorded.dispatch::<Ledger>(&cover("account", 1), &registry, &mut []),
        Err(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        })
    );
    assert_eq!(
        recorded.dispatch::<Ledger>(&cover("ledger", 9), &registry, &mut []),
        Ok(()),
        "a stream the chain did not touch: nothing recorded"
    );
}

#[test]
fn a_duplicate_runs_no_projection_and_commits_nothing() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let mut deposits = Deposits::default();
    let mut route = crate::ProjectionRoute {
        name: "deposits",
        projection: &mut deposits,
        domain: std::marker::PhantomData,
    };
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    let handled = kernel
        .handle::<Account, _, _>(&store, open, &at(30), &mut route)
        .ready()
        .unwrap();
    assert!(matches!(handled, crate::Handled::Duplicate(_)));
    assert_eq!(handled.outcome().projector_failures, []);
    assert!(deposits.0.is_empty());
}

/// The handler's future is `Send` over a store whose futures are, so a
/// server spawns it on a multi-threaded runtime.
#[test]
fn handle_is_a_send_future_over_a_send_store() {
    fn send<T: Send>(t: T) -> T {
        t
    }
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let handled = send(kernel.handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ()))
        .ready()
        .unwrap();
    assert_eq!(handled.books().len(), 2);
    let resumed = send(kernel.resume(&store, &at(31), &mut ()))
        .ready()
        .unwrap();
    assert!(resumed.books().is_empty(), "nothing owed");
}

/// The account stream as stored.
fn account_stream(store: &MemoryStore, registry: &crate::Registry) -> crate::Stream<Account> {
    let pages = pages(store, &cover("account", 1));
    crate::Stream::replay(cover("account", 1), &pages, registry).unwrap()
}

/// Account deposits → the ledger, and nothing else: a policy that targets
/// from the event, so the ledger is owed only some of the account's events.
struct DepositsOnly;

impl crate::Policy for DepositsOnly {
    const NAME: &'static str = "deposits-only";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        let Ok(crate::toys::AccountEvent::Deposited(cents)) =
            crate::toys::AccountEvent::from_any(event)
        else {
            return Ok(None);
        };
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents,
        };
        Ok(Some(
            crate::markup::Commands::none().send(ledger_root(), &record),
        ))
    }
}

/// Account deposits → the ledger, but a deposit of 13 cents cannot happen:
/// the policy answers it with its error, a bug, not a command.
struct Unlucky;

impl crate::Policy for Unlucky {
    const NAME: &'static str = "unlucky";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        let Ok(crate::toys::AccountEvent::Deposited(cents)) =
            crate::toys::AccountEvent::from_any(event)
        else {
            return Ok(None);
        };
        if cents == 13 {
            return Err(crate::markup::Report::new("toy-unlucky-deposit").arg("cents", cents));
        }
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents,
        };
        Ok(Some(
            crate::markup::Commands::none().send(ledger_root(), &record),
        ))
    }
}

/// A policy's error is a bug its step cannot run past: the step is
/// dead-lettered with the error's code as its reason and its args, never
/// its English, the call fails downstream with it as
/// [`StepError::PolicyFailed`], a defect (not a business rejection), and
/// the policy's position moves past the trigger, as for a refused
/// command's step, so nothing runs it again. The command's commit stands,
/// and the next event is handled as usual.
#[test]
fn a_policys_error_dead_letters_its_step_and_moves_its_position_past_it() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Unlucky);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;

    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, 13), &at(30), &mut ())
        .ready()
        .err()
        .expect("the policy's error fails the call");
    let Failure::Downstream { error, .. } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    let StepError::PolicyFailed(report) = error else {
        panic!("the policy's error, its own kind: {error:?}");
    };
    assert_eq!(
        angzarr_diagnostic::Diagnostic::code(report),
        "toy-unlucky-deposit"
    );
    assert_eq!(
        failure.recovery(),
        crate::Recovery::Bug,
        "a defect, alertable, not a business refusal"
    );
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the command's deposit committed, and stays"
    );
    assert_eq!(pages(&store, &ledger_cover()), [], "the step sent nothing");
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "the failed step, dead-lettered: {lines:?}");
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    assert!(
        lines[0].contains("unlucky: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unlucky-deposit cause.cents=13"),
        "the policy, its bug's key, its error's code as the cause, and its args: {}",
        lines[0]
    );
    // A policy's error is a handler's bug, keyed as the router keys one;
    // the trigger is the payload, processing_failed, never retried, not
    // transient (decision 14.48).
    let letter = crate::toys::one_letter(log.letters());
    let reason = crate::toys::wire(
        "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
        &[("cause", "toy-unlucky-deposit"), ("cause.cents", "13")],
    );
    crate::toys::assert_letter(
        &letter,
        crate::v1::ComponentKind::Policy,
        &reason,
        crate::toys::processing_failed(&reason, 0, false),
    );
    assert!(
        matches!(
            &letter.payload,
            Some(crate::v1::angzarr_dead_letter::Payload::RejectedEvents(book))
                if book.cover == Some(cover("account", 1))
        ),
        "{letter:?}"
    );
    assert!(
        !lines[0].contains("cannot happen"),
        "no English: {}",
        lines[0]
    );
    assert_eq!(
        position(&store, "unlucky", &cover("account", 1)),
        before + 1,
        "past the trigger it failed on"
    );

    kernel
        .handle::<Account, _, _>(&store, deposit(4, 5), &at(31), &mut ())
        .ready()
        .expect("the next event is handled as usual");
    let book = crate::v1::EventBook {
        pages: pages(&store, &ledger_cover()),
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(before + 1, 5)],
        "the next deposit, and only it"
    );
    assert_eq!(
        position(&store, "unlucky", &cover("account", 1)),
        before + 2
    );
    assert_eq!(log.lines().len(), 1, "nothing more dead-lettered");
}

/// What a kernel does with a policy's bug unless told otherwise: it moves
/// the policy's position past the trigger.
#[test]
fn a_kernel_moves_a_policys_position_past_its_bug_by_default() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    assert_eq!(OnPolicyBug::default(), OnPolicyBug::MovePast);
    assert_eq!(
        Kernel::new(&registry, &log).policy_bug(),
        OnPolicyBug::MovePast
    );
    assert_eq!(
        Kernel::new(&registry, &log)
            .on_policy_bug(OnPolicyBug::Stay)
            .policy_bug(),
        OnPolicyBug::Stay
    );
}

/// Under [`OnPolicyBug::Stay`], a policy's error is still dead-lettered
/// and fails the call as [`StepError::PolicyFailed`], but the policy's
/// position stays at the trigger, so a resume runs the step again (and
/// dead-letters it again).
#[test]
fn under_stay_a_policys_error_holds_its_position() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Unlucky);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_policy_bug(OnPolicyBug::Stay);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let held = position(&store, "unlucky", &cover("account", 1));

    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, 13), &at(30), &mut ())
        .ready()
        .err()
        .expect("the policy's error fails the call");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(log.lines().len(), 1, "dead-lettered: {:?}", log.lines());
    assert_eq!(
        position(&store, "unlucky", &cover("account", 1)),
        held,
        "the position stays"
    );

    let resumed = kernel.resume(&store, &at(31), &mut ()).ready();
    assert!(
        matches!(
            &resumed,
            Err(Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            })
        ),
        "the step runs again: {:?}",
        resumed.as_ref().err()
    );
    assert_eq!(log.lines().len(), 2, "dead-lettered again");
}

/// Under [`OnPolicyBug::Stay`], a policy's refusal answer that fails is
/// still dead-lettered as [`StepError::PolicyFailed`], but the policy's
/// position stays at the trigger.
#[test]
fn under_stay_a_policys_refusal_answer_error_holds_its_position() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(UnansweringBookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_policy_bug(OnPolicyBug::Stay);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let held = position(&store, "unanswering-bookkeeping", &cover("account", 1));
    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30), &mut ())
        .ready()
        .err()
        .expect("the answer's error fails the call");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(log.lines().len(), 1, "dead-lettered: {:?}", log.lines());
    assert_eq!(
        position(&store, "unanswering-bookkeeping", &cover("account", 1)),
        held,
        "the position stays"
    );
}

#[test]
fn a_filtered_policy_reaches_its_target_only_for_the_events_it_targets() {
    let mut registry = registry();
    registry.policy(DepositsOnly);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    for book in [
        command(cover("account", 1), AccountCmd::Open.to_any()).id(1),
        deposit(2, 5),
        command(cover("account", 1), AccountCmd::Withdraw(2).to_any()).id(3),
        deposit(4, 7),
    ] {
        kernel
            .handle::<Account, _, _>(&store, book, &at(30), &mut ())
            .ready()
            .unwrap();
    }
    let book = crate::v1::EventBook {
        pages: pages(&store, &ledger_cover()),
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(1, 5), (3, 7)],
        "the deposits, and only the deposits, in source order"
    );
    assert_eq!(
        position(&store, "deposits-only", &cover("account", 1)),
        4,
        "past every event, the untargeted included"
    );
}

#[test]
fn a_policy_book_for_an_unregistered_target_is_a_dead_lettered_bug() {
    let mut registry = crate::Registry::new();
    registry.policy(Bookkeeping);
    registry
        .register::<Account>(
            crate::Upcaster::new("u", "account"),
            crate::toys::handlers::<crate::toys::Account>(),
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let failure = kernel
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream { path, error } = &failure else {
        panic!("downstream: {failure:?}");
    };
    assert_eq!(path.len(), 1);
    assert_eq!(
        error,
        &StepError::Kernel(KernelError::Malformed(crate::Malformed::UnknownDomain {
            domain: "ledger".to_string()
        }))
    );
    assert_eq!(
        failure.recovery(),
        Recovery::Bug,
        "the kernel's book: a bug"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    assert!(
        lines[0]
            .contains("bookkeeping: COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN command_type=toy.ledger.cmd.v1.Record domain=ledger"),
        "{}",
        lines[0]
    );
    assert_eq!(
        streams(&store),
        [cover("account", 1)],
        "the command's commit stands"
    );
    assert_eq!(
        position(&store, "bookkeeping", &cover("account", 1)),
        0,
        "a bug holds the position, so the step runs again once it is fixed"
    );
}

/// A command's policy steps are owed for its own events only: the second
/// deposit's chain runs one ledger step, for its own event, not the first's
/// again.
#[test]
fn a_command_owes_policy_steps_for_its_own_events_only() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    opened(&store, &plain, 1, 0);
    for n in [3, 4] {
        kernel
            .handle::<Account, _, _>(&store, deposit(n, 1), &at(30), &mut ())
            .ready()
            .unwrap();
    }
    let ledger = slice(store.inner(), &ledger_cover()).pages;
    let book = crate::v1::EventBook {
        pages: ledger,
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| r.source_sequence)
            .collect::<Vec<_>>(),
        [2, 3],
        "each deposit's own event, once"
    );
    assert_eq!(
        store.landed()[2..],
        [
            Some(cover("account", 1)),
            Some(ledger_cover()),
            Some(cover("account", 1)),
            Some(ledger_cover()),
        ],
        "after the two opening commands: per deposit, its command, then one ledger step"
    );
}

#[test]
fn a_chain_on_a_stream_loaded_from_a_snapshot_records_only_its_events() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 7);
    let source = account_stream(&store, &registry);
    store
        .write_snapshot(
            sref(&cover("account", 1)),
            &source.snapshot(&registry).unwrap(),
        )
        .ready()
        .unwrap();
    let (mut one, mut two) = (Deposits::default(), Deposits::default());
    let first = crate::ProjectionRoute {
        name: "first",
        projection: &mut one,
        domain: std::marker::PhantomData,
    };
    let second = crate::ProjectionRoute {
        name: "second",
        projection: &mut two,
        domain: std::marker::PhantomData,
    };
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut (first, second))
        .ready()
        .unwrap();
    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert!(recorded.projector_failures().is_empty());
    let events = recorded.events::<Account>(&cover("account", 1)).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence(), 2);
    let seen = [one.0, two.0];
    assert_eq!(seen, [vec![25], vec![25]], "both projections ran");
}

#[test]
fn a_committed_event_names_its_stream() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 7);
    let source = account_stream(&store, &registry);
    let mut covers = Vec::new();
    let mut handler = |c: &crate::Committed<'_, Account>| {
        covers.push((c.cover().clone(), c.envelope().sequence()))
    };
    source.dispatch(1, &registry, &mut [&mut handler]).unwrap();
    assert_eq!(covers, [(cover("account", 1), 1)]);
}

/// A policy as a handler of its source hands over one order per target,
/// in target order: each indexed by its place, keeping the trigger's
/// correlation.
#[test]
fn a_policys_orders_are_indexed_and_keep_the_triggers_correlation() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let source = account_stream(&store, &registry);
    let mut orders = Vec::new();
    let log = MemoryDeadLetters::new();
    let mut handler = crate::policy(&TwoLedgers, &log, |order| orders.push(order));
    source.dispatch(1, &registry, &mut [&mut handler]).unwrap();
    assert_eq!(
        orders
            .iter()
            .map(|o| (o.index(), o.cover().clone(), o.trigger().sequence))
            .collect::<Vec<_>>(),
        [(0, two_ledgers(0), 1), (1, two_ledgers(1), 1)]
    );
    let correlation = source.events()[1].correlation_id().clone();
    assert!(orders.iter().all(|o| *o.correlation_id() == correlation));
    assert_ne!(correlation, crate::v1::Uuid::default());
}

/// A policy's error, on the handler path, hands over no order: the step
/// is dead-lettered as the kernel's is, the policy named, its error's code
/// as the reason and its args, never its English.
#[test]
fn a_policys_error_as_a_handler_of_its_source_is_dead_lettered() {
    let registry = registry();
    let unused = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &unused);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let deposited = slice(&store, &cover("account", 1)).next_sequence;
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 13), &at(30), &mut ())
        .ready()
        .expect("deposited, with no policy registered");
    let source = account_stream(&store, &registry);
    let log = MemoryDeadLetters::new();
    let mut orders = Vec::new();
    let mut handler = crate::policy(&Unlucky, &log, |order| orders.push(order));
    source
        .dispatch(deposited, &registry, &mut [&mut handler])
        .unwrap();
    assert!(orders.is_empty(), "no order for the event it failed on");
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].starts_with(crate::DEAD_LETTER_PREFIX));
    assert!(
        lines[0].contains(&format!(
            "unlucky: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unlucky-deposit cause.cents=13 sequence={deposited} stream=account/{}",
            cover("account", 1).root.unwrap()
        )),
        "the policy, its bug's key, its error's code and args, and the trigger: {}",
        lines[0]
    );
    let reason = crate::toys::wire(
        "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
        &[("cause", "toy-unlucky-deposit"), ("cause.cents", "13")],
    );
    crate::toys::assert_letter(
        &crate::toys::one_letter(log.letters()),
        crate::v1::ComponentKind::Policy,
        &reason,
        crate::toys::processing_failed(&reason, 0, false),
    );
    assert!(!lines[0].contains("cannot happen"), "{}", lines[0]);
}

/// Two ledgers per account event: orders are indexed in target order.
struct TwoLedgers;

impl crate::Policy for TwoLedgers {
    const NAME: &'static str = "two-ledgers";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        _: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents: 0,
        };
        let commands = crate::markup::Commands::none()
            .send(two_ledgers_root(0), &record)
            .send(two_ledgers_root(1), &record);
        Ok(Some(commands))
    }
}

/// The root of the `n`th of [`TwoLedgers`]' ledgers.
fn two_ledgers_root(n: u8) -> crate::v1::Uuid {
    ids::compute_root("ledger", &n.to_string()).unwrap()
}

#[test]
fn a_triggers_commands_are_indexed_in_target_order() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(TwoLedgers);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 5), &at(30), &mut ())
        .ready()
        .unwrap();
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    for index in [0u8, 1] {
        let pages = pages(&store, &two_ledgers(index));
        assert_eq!(
            pages[0].meta.as_ref().unwrap().causation,
            Some(Causation::CommandId(
                ids::policy_command_id(&trigger, "two-ledgers", u32::from(index)).unwrap()
            )),
            "target {index}'s command is the trigger's {index}th"
        );
    }
}

/// The `n`th of [`TwoLedgers`]' ledgers.
fn two_ledgers(n: u8) -> crate::v1::Cover {
    crate::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ids::compute_root("ledger", &n.to_string()).unwrap()),
        edition: None,
    }
}

/// Reaction fan-out (the owner's addition): several policies on one source
/// domain, through the builder, each run for every event of that domain, in
/// the order they were registered (per event, every policy; per policy,
/// every target), each as its own committed step, after the command's own.
#[test]
fn every_policy_on_a_source_runs_for_each_of_its_events_in_registration_order() {
    let mut registry = registry();
    registry.policy(TwoLedgers);
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    for (book, second) in [(open, 30), (deposit(2, 5), 31)] {
        kernel
            .handle::<Account, _, _>(&store, book, &at(second), &mut ())
            .ready()
            .unwrap();
    }
    let per_event = [
        Some(cover("account", 1)),
        Some(two_ledgers(0)),
        Some(two_ledgers(1)),
        Some(ledger_cover()),
    ];
    assert_eq!(
        store.landed(),
        [per_event.clone(), per_event].concat(),
        "per event: the command, TwoLedgers' targets in order, then Bookkeeping's"
    );
    for target in [two_ledgers(0), two_ledgers(1), ledger_cover()] {
        let book = crate::v1::EventBook {
            pages: slice(store.inner(), &target).pages,
            ..Default::default()
        };
        assert_eq!(
            payloads::<ledger::Recorded>(&[book])
                .iter()
                .map(|r| r.source_sequence)
                .collect::<Vec<_>>(),
            [0, 1],
            "{target:?} reached for each account event"
        );
    }
}

/// A policy step's events land with the policy's position on its source:
/// one write, and the command's own write puts no position. A policy with
/// several targets puts it once, with the trigger's last target, so a
/// crash between its targets leaves the trigger owed (the targets that
/// landed are deduplicated when it runs again).
#[test]
fn a_policy_step_commits_its_position_with_its_events() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(TwoLedgers);
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 5), &at(30), &mut ())
        .ready()
        .unwrap();
    let writes = store.writes();
    let account = cover("account", 1);
    let [command, first, last, bookkeeping] = &writes[writes.len() - 4..] else {
        panic!("four writes for the deposit: {writes:?}");
    };
    assert_eq!(command.wrote, Some((account.clone(), 1)));
    assert_eq!(command.positions, [], "a command's write puts no position");
    assert_eq!(first.wrote, Some((two_ledgers(0), 1)));
    assert_eq!(first.positions, [], "not the trigger's last target");
    assert_eq!(last.wrote, Some((two_ledgers(1), 1)));
    assert_eq!(
        last.positions,
        [("two-ledgers".to_string(), account.clone(), 3)]
    );
    assert_eq!(bookkeeping.wrote, Some((ledger_cover(), 1)));
    assert_eq!(
        bookkeeping.positions,
        [("bookkeeping".to_string(), account.clone(), 3)]
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 3);
}

/// A step the store fails (here, its write) keeps every write before it,
/// and writes nothing more: its policy's position stays where the last
/// committed step put it. The account is opened with the policy registered,
/// so the position past the opening is an ordinary step's. The call fails
/// with the store's error, to be retried; `Kernel::resume` runs what is
/// owed from each policy's position, and running it again finds nothing
/// more.
#[test]
fn a_step_the_store_failed_is_resumed_by_position() {
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_retryable(crate::toys::no_retries());
    let store = Recording::new();
    opened(&store, &registry, 1, 0);
    let account = cover("account", 1);
    assert_eq!(
        position(store.inner(), "bookkeeping", &account),
        2,
        "the opening's own ledger steps put it"
    );
    let before = store.writes().len();
    store.fail_write(1);
    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::Kernel(KernelError::Store(crate::StoreError::Unavailable(_))),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(
        pages(store.inner(), &account).len(),
        3,
        "the deposit stands"
    );
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        2,
        "the opening's two records; the deposit's step landed nothing"
    );
    let writes = store.writes();
    assert_eq!(
        writes[before..]
            .iter()
            .map(|w| (w.wrote.clone(), w.landed))
            .collect::<Vec<_>>(),
        [
            (Some((account.clone(), 1)), true),
            (Some((ledger_cover(), 1)), false),
        ],
        "the command's write, then the step's failed one, and nothing after it"
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 2);

    let resumed = kernel.resume(&store, &at(31), &mut ()).ready().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(resumed.books()),
        [ledger::Recorded {
            source_sequence: 2,
            cents: 25
        }]
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 3);
    let again = kernel.resume(&store, &at(32), &mut ()).ready().unwrap();
    assert!(again.books().is_empty(), "nothing more owed");
    assert_eq!(pages(store.inner(), &ledger_cover()).len(), 3);
}

/// Account events → a sweep of account 9, which holds nothing: a policy
/// whose target decides no event.
struct SweepNine;

impl crate::Policy for SweepNine {
    const NAME: &'static str = "sweep-nine";
    type Source = Account;
    type Target = Account;

    fn on_event(
        &self,
        _: &crate::Any,
        _: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Account>>, crate::markup::Report> {
        let root = cover("account", 9).root.unwrap();
        Ok(Some(
            crate::markup::Commands::none().send(root, &AccountCmd::Sweep),
        ))
    }
}

/// A policy step whose target decides no event writes no event, but still
/// puts its policy's position past the trigger, alone: the trigger is
/// handled, and nothing is owed.
#[test]
fn a_policy_step_that_decides_no_event_still_moves_its_position() {
    let mut registry = registry();
    registry.policy(SweepNine);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    kernel
        .handle::<Account, _, _>(&store, open, &at(30), &mut ())
        .ready()
        .unwrap();
    let account = cover("account", 1);
    assert_eq!(
        streams(store.inner()),
        [cover("account", 1)],
        "the sweep wrote no event"
    );
    assert_eq!(
        store.writes(),
        [
            crate::recording::WriteRecord {
                wrote: Some((account.clone(), 1)),
                positions: vec![],
                landed: true,
            },
            crate::recording::WriteRecord {
                wrote: None,
                positions: vec![("sweep-nine".to_string(), account.clone(), 1)],
                landed: true,
            },
        ],
        "the command's write, then the step's position alone"
    );
    assert_eq!(position(store.inner(), "sweep-nine", &account), 1);
    let resumed = kernel.resume(&store, &at(31), &mut ()).ready().unwrap();
    assert!(resumed.books().is_empty(), "nothing owed");
    assert_eq!(store.writes().len(), 2, "and nothing run again");
}

/// Account deposits → the ledger, answering its refusals: every refusal it
/// is handed, kept.
/// The refusals a policy was handed, each with the refused command's
/// context.
type Answered = std::sync::Arc<
    std::sync::Mutex<Vec<(crate::markup::v1::Refusal, crate::markup::CommandContext)>>,
>;

struct AnsweringBookkeeping(Answered);

impl crate::Policy for AnsweringBookkeeping {
    const NAME: &'static str = "answering-bookkeeping";
    type Source = Account;
    type Target = Ledger;
    const ANSWERS_REFUSALS: bool = true;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        crate::Policy::on_event(&Bookkeeping, event, ctx)
    }

    fn on_refused(
        &self,
        refusal: &crate::markup::v1::Refusal,
        ctx: &crate::markup::CommandContext,
    ) -> Result<crate::Any, crate::markup::Report> {
        self.0.lock().unwrap().push((refusal.clone(), ctx.clone()));
        Ok(crate::markup::pass_through(refusal))
    }
}

/// As [`Bookkeeping`], answering refusals; but a refusal of a record for
/// more than its limit cannot happen, so its answer is its error, a bug.
struct UnansweringBookkeeping;

impl crate::Policy for UnansweringBookkeeping {
    const NAME: &'static str = "unanswering-bookkeeping";
    type Source = Account;
    type Target = Ledger;
    const ANSWERS_REFUSALS: bool = true;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<Ledger>>, crate::markup::Report> {
        crate::Policy::on_event(&Bookkeeping, event, ctx)
    }

    fn on_refused(
        &self,
        refusal: &crate::markup::v1::Refusal,
        _ctx: &crate::markup::CommandContext,
    ) -> Result<crate::Any, crate::markup::Report> {
        let trigger = refusal
            .event
            .as_ref()
            .and_then(|book| book.pages.first())
            .map_or(0, |page| page.sequence);
        Err(crate::markup::Report::new("toy-unanswerable-refusal").arg("trigger", trigger))
    }
}

/// A policy's refusal answer reads the refused command's context, whole,
/// as its target's handler was handed it: the target's stream, its next
/// sequence and history, the sequence and resolution it was decided under,
/// the command's derived id and the trigger's correlation.
#[test]
fn a_policys_refusal_answer_reads_the_refused_commands_context() {
    let plain = registry();
    let mut registry = registry();
    let refusals = Answered::default();
    registry.policy(AnsweringBookkeeping(std::sync::Arc::clone(&refusals)));
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 5), &at(30), &mut ())
        .ready()
        .unwrap();
    kernel
        .handle::<Account, _, _>(&store, deposit(4, LEDGER_LIMIT + 1), &at(31), &mut ())
        .ready()
        .unwrap();
    let refusals = refusals.lock().unwrap();
    let [(_, ctx)] = refusals.as_slice() else {
        panic!("one refusal: {refusals:?}");
    };
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 3,
    };
    assert_eq!(
        (
            ctx.next_sequence,
            ctx.had_prior_events,
            ctx.expected_sequence
        ),
        (1, true, 1),
        "the ledger as its handler saw it: one record before, decided at its head"
    );
    assert_eq!(
        ctx.conflict_resolution,
        crate::v1::ConflictResolution::Strict
    );
    assert!(ctx.conflict.is_none(), "STRICT has no conflict");
    assert_eq!(
        ctx.cover,
        Some(ledger_cover()),
        "the refused command's target"
    );
    assert_eq!(
        ctx.command_id,
        Some(ids::policy_command_id(&trigger, "answering-bookkeeping", 0).unwrap()),
        "the refused command's derived id"
    );
    let correlation = pages(&store, &cover("account", 1))[3]
        .meta
        .clone()
        .and_then(|meta| meta.correlation_id);
    assert!(correlation.is_some());
    assert_eq!(ctx.correlation_id, correlation, "the trigger's correlation");
}

/// A policy's refusal answer that fails is a bug: the step fails as
/// [`StepError::PolicyFailed`], a defect, is dead-lettered by the error's
/// code and args with no English, writes no refusal fact, and the
/// policy's position moves past the trigger, so the next event is handled
/// as usual.
#[test]
fn a_policys_refusal_answer_error_dead_letters_as_policy_failed() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(UnansweringBookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;
    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30), &mut ())
        .ready()
        .err()
        .expect("the answer's error fails the call");
    let Failure::Downstream { error, .. } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    let StepError::PolicyFailed(report) = error else {
        panic!("the policy's failure: {error:?}");
    };
    assert_eq!(
        angzarr_diagnostic::Diagnostic::code(report),
        "toy-unanswerable-refusal"
    );
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the deposit stays, and no refusal fact was recorded"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].contains(&format!(
            "unanswering-bookkeeping: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-unanswerable-refusal cause.trigger={before}"
        )),
        "{}",
        lines[0]
    );
    let reason = crate::toys::wire(
        "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
        &[
            ("cause", "toy-unanswerable-refusal"),
            ("cause.trigger", &before.to_string()),
        ],
    );
    crate::toys::assert_letter(
        &crate::toys::one_letter(log.letters()),
        crate::v1::ComponentKind::Policy,
        &reason,
        crate::toys::processing_failed(&reason, 0, false),
    );
    assert!(!lines[0].contains("cannot happen"), "{}", lines[0]);
    assert_eq!(
        position(&store, "unanswering-bookkeeping", &cover("account", 1)),
        before + 1,
        "past the trigger"
    );

    kernel
        .handle::<Account, _, _>(&store, deposit(4, 5), &at(31), &mut ())
        .ready()
        .expect("the next event is handled as usual");
    assert_eq!(
        position(&store, "unanswering-bookkeeping", &cover("account", 1)),
        before + 2
    );
    assert_eq!(log.lines().len(), 1, "nothing more dead-lettered");
}

/// A refusal is handed to its policy with the triggering event: its
/// stream, and its page as stored, at its own sequence (here 2, past the
/// opening).
#[test]
fn a_refusal_carries_its_triggering_event() {
    let plain = registry();
    let mut registry = registry();
    let refusals = std::sync::Arc::default();
    registry.policy(AnsweringBookkeeping(std::sync::Arc::clone(&refusals)));
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30), &mut ())
        .ready()
        .unwrap();
    let account = cover("account", 1);
    let refusals = refusals.lock().unwrap();
    let [(refusal, _)] = refusals.as_slice() else {
        panic!("one refusal: {refusals:?}");
    };
    let event = refusal.event.clone().unwrap();
    assert_eq!(event.cover, Some(account.clone()));
    assert_eq!(
        event.pages,
        [pages(&store, &account)[2].clone()],
        "the deposit's page, as stored"
    );
    assert_eq!(event.pages[0].sequence, 2);
}

/// A crash between a command's commit and its policy steps leaves them
/// owed. A client's retry of the command is answered as a duplicate, and
/// settles what its stream still owes first, so the retry finishes the
/// chain.
#[test]
fn a_retry_after_a_crash_settles_what_its_stream_still_owes() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_retryable(crate::toys::no_retries());
    let store = Recording::new();
    opened(&store, &plain, 1, 0);
    store.fail_write(1);
    assert!(kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .is_err());
    let retried = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(31), &mut ())
        .ready()
        .unwrap();
    assert!(matches!(retried, crate::Handled::Duplicate(_)));
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        1,
        "the owed step ran"
    );
    assert_eq!(
        position(store.inner(), "bookkeeping", &cover("account", 1)),
        3
    );
}

/// A policy added to a domain with history owes it everything: `resume`
/// runs it from the start (position 0), one step per event.
#[test]
fn resume_runs_a_new_policy_over_its_sources_history() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 7);
    opened(&store, &plain, 2, 9);
    let resumed = kernel.resume(&store, &at(40), &mut ()).ready().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(resumed.books())
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 7), (0, 0), (1, 9)],
        "every account's events, account by account, in order"
    );
    for n in [1, 2] {
        assert_eq!(position(&store, "bookkeeping", &cover("account", n)), 2);
    }
}

/// A step that landed before its position was put (or a position put back)
/// is run again from the position and deduplicated by its derived command
/// id: nothing lands twice.
#[test]
fn a_resumed_step_that_already_landed_is_a_duplicate() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel.resume(&store, &at(30), &mut ()).ready().unwrap();
    let account = cover("account", 1);
    let key = crate::PositionKey {
        handler: "bookkeeping",
        stream: sref(&account),
    };
    store.write(Write::positions(&[(key, 0)])).ready().unwrap();
    let resumed = kernel.resume(&store, &at(31), &mut ()).ready().unwrap();
    assert!(resumed.books().iter().all(|b| b.pages.is_empty()));
    assert_eq!(pages(&store, &ledger_cover()).len(), 2, "nothing twice");
    assert_eq!(position(&store, "bookkeeping", &account), 2);
}

/// A command whose write the store fails is the store's error, and nothing
/// is written (or run after it).
#[test]
fn a_store_that_cannot_write_fails_the_command_with_nothing_written() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_retryable(crate::toys::no_retries());
    let store = Recording::new();
    store.fail_write(0);
    let failure = kernel
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::Store(crate::StoreError::Unavailable(_)))
    ));
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(streams(store.inner()), []);
}

/// A domain named "account" that is not the account: a projection routed
/// through it finds the chain's account stream of another type.
struct Impostor;

impl crate::Domain for Impostor {
    type State = crate::toys::LedgerState;
    type Command = std::convert::Infallible;
    type Fact = std::convert::Infallible;
    type Event = std::convert::Infallible;
    type Rejection = std::convert::Infallible;
    const RULES_VERSION: &'static str = "v1";

    const NAME: &'static str = "account";

    fn apply(_: &mut crate::toys::LedgerState, event: &std::convert::Infallible) {
        match *event {}
    }
}

struct Unseen;

impl crate::Projection<Impostor> for Unseen {
    fn project(&mut self, _: &crate::Committed<'_, Impostor>) {}
}

#[test]
fn a_projection_that_fails_is_reported_and_the_commit_stands() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let mut deposits = Deposits::default();
    let mut routes = (
        crate::ProjectionRoute {
            name: "impostor",
            projection: &mut Unseen,
            domain: std::marker::PhantomData::<fn() -> Impostor>,
        },
        crate::ProjectionRoute {
            name: "deposits",
            projection: &mut deposits,
            domain: std::marker::PhantomData,
        },
    );
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut routes)
        .ready()
        .unwrap();
    let failure = crate::ProjectorFailure {
        projector: "impostor",
        error: KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "account",
        },
    };
    assert_eq!(handled.outcome().projector_failures.len(), 1);
    let crate::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert_eq!(recorded.projector_failures(), [failure]);
    assert_eq!(deposits.0, [25], "the other projection still ran");
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        3,
        "the commit stands"
    );
}

/// Waits that complete at once, kept; every jitter draw 0.
#[derive(Default)]
struct Waits(std::sync::Mutex<Vec<std::time::Duration>>);

impl crate::Wait for Waits {
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// A policy's step whose write fails once with `error` is reloaded and
/// run again after one backoff: it lands, and nothing is dead-lettered.
fn a_policy_step_failing_once_lands_on_its_retry(error: crate::StoreError) {
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry, &log).wait_with(&waits);
    let store = Recording::new();
    opened(&store, &registry, 1, 0);
    store.fail_write_with(1, error);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .unwrap();
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        3,
        "the deposit's step landed on its retry"
    );
    assert_eq!(waits.0.lock().unwrap().len(), 1);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// A policy's step whose write lost a race (a store conflict, ABORTED) has
/// the kernel for its caller: reloaded and run again.
#[test]
fn a_policy_steps_lost_race_is_reloaded_and_run_again() {
    a_policy_step_failing_once_lands_on_its_retry(crate::StoreError::Conflict {
        stream: Box::new(ledger_cover()),
        expected: 2,
        actual: 3,
    });
}

/// A policy's step whose store could not say is a retryable bug: run again.
#[test]
fn a_policy_steps_unavailable_store_is_run_again() {
    a_policy_step_failing_once_lands_on_its_retry(crate::StoreError::Unavailable(
        angzarr_ports::Cause::new("angzarr-store-sqlite-unavailable", []),
    ));
}

/// The kernel's own jitter draws are uniform in `[0, 1)`: in range, spread
/// over it, and not repeating.
#[test]
fn the_kernels_own_draws_are_uniform_in_the_unit_interval() {
    use crate::Wait;
    let draws: Vec<f64> = (0..2000).map(|_| super::ThreadWait.draw()).collect();
    assert!(draws.iter().all(|d| (0.0..1.0).contains(d)), "{draws:?}");
    let mean = draws.iter().sum::<f64>() / draws.len() as f64;
    assert!((0.4..0.6).contains(&mean), "{mean}");
    let distinct: std::collections::BTreeSet<u64> = draws.iter().map(|d| d.to_bits()).collect();
    assert!(distinct.len() > 1900, "{}", distinct.len());
}

/// The kernel's own wait completes once its backoff has passed, waking the
/// task that waits.
#[test]
fn the_kernels_own_wait_waits_out_its_backoff() {
    use crate::Wait;
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(std::sync::Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let backoff = std::time::Duration::from_millis(30);
    let started = std::time::Instant::now();
    let mut wait = super::ThreadWait.wait(backoff);
    let mut polls = 0;
    while wait.as_mut().poll(&mut cx) == Poll::Pending {
        polls += 1;
        std::thread::park_timeout(std::time::Duration::from_secs(5));
    }
    assert!(started.elapsed() >= backoff, "{:?}", started.elapsed());
    assert!(polls >= 1, "pending until the backoff passed");
}

/// Without threads (`wasm32-unknown-unknown`, which panics on a thread's
/// spawn), the kernel's own wait completes at once, whatever the backoff,
/// so a retry never panics there: a retry runs straight after the failure,
/// and an application that wants the backoff waited out injects its
/// timer ([`Kernel::wait_with`]). Its draws are the kernel's own, uniform
/// in `[0, 1)`. With threads, the wait is the thread's: still pending at
/// its first poll.
#[test]
fn without_threads_the_kernels_own_wait_completes_at_once() {
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    let hour = std::time::Duration::from_secs(3600);

    let mut unthreaded = super::default_wait(false).wait(hour);
    assert_eq!(unthreaded.as_mut().poll(&mut cx), Poll::Ready(()));
    let draws: Vec<f64> = (0..64).map(|_| super::default_wait(false).draw()).collect();
    assert!(draws.iter().all(|d| (0.0..1.0).contains(d)), "{draws:?}");
    let distinct: std::collections::BTreeSet<u64> = draws.iter().map(|d| d.to_bits()).collect();
    assert!(distinct.len() > 1, "random, not one value: {draws:?}");

    let mut threaded = super::default_wait(true).wait(hour);
    assert_eq!(threaded.as_mut().poll(&mut cx), Poll::Pending);
}

/// A command with no expected sequence whose write lost a race (the
/// store's conflict: another write to its stream landed first) is the
/// kernel's own race: it is reloaded and decided again on the new head,
/// after one backoff under the retry policy, for an outside caller too
/// (decision 14.45). It lands, and nothing is dead-lettered.
#[test]
fn an_outside_callers_lost_race_without_an_expected_sequence_is_run_again() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry, &log).wait_with(&waits);
    let store = Recording::new();
    opened(&store, &registry, 1, 0);
    store.fail_write_with(
        0,
        crate::StoreError::Conflict {
            stream: Box::new(cover("account", 1)),
            expected: 2,
            actual: 3,
        },
    );
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready();
    assert!(
        matches!(handled, Ok(crate::Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    assert_eq!(
        *waits.0.lock().unwrap(),
        [crate::OnRetryable::default().initial_backoff],
        "run again once, after the first backoff"
    );
    assert_eq!(pages(&store, &cover("account", 1)).len(), 3);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// A command under STRICT whose sender set its expected sequence, and
/// whose write lost a race, decided against a head that is gone: the
/// stale expected sequence, against the head the store found, back to its
/// outside caller at once: never run again, never dead-lettered (decision
/// 14.45).
#[test]
fn an_outside_callers_lost_race_with_an_expected_sequence_is_stale_at_once() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry, &log).wait_with(&waits);
    let store = Recording::new();
    opened(&store, &registry, 1, 0);
    store.fail_write_with(
        0,
        crate::StoreError::Conflict {
            stream: Box::new(cover("account", 1)),
            expected: 2,
            actual: 3,
        },
    );
    let failure = kernel
        .handle::<Account, _, _>(
            &store,
            deposit(3, 25).expecting(2, crate::v1::ConflictResolution::Strict),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .expect("the race was lost");
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Stale {
            stream: cover("account", 1),
            expected: 2,
            actual: 3,
        })
    );
    assert!(waits.0.lock().unwrap().is_empty(), "not retried");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// A policy of the account's events, to the ledger, behind a router that
/// checks nothing: it answers whatever `answer` builds for the triggering
/// event, as a remote policy's business code might (decision 14.42).
struct Unchecked {
    answer: fn(&crate::v1::EventBook) -> Vec<CommandBook>,
}

impl crate::angzarr_router::router::PolicyComponent for Unchecked {
    fn name(&self) -> &str {
        "unchecked"
    }
    fn input_domain(&self) -> &str {
        "account"
    }
    async fn dispatch(
        &self,
        source: &crate::v1::EventBook,
    ) -> Result<crate::angzarr_router::pb::PolicyResponse, crate::angzarr_router::error::CodedError>
    {
        Ok(crate::angzarr_router::pb::PolicyResponse {
            reactions: vec![crate::angzarr_router::pb::PolicyReaction {
                trigger_sequence: source.pages[0].sequence,
                commands: (self.answer)(source),
            }],
        })
    }
    async fn dispatch_refusal(
        &self,
        _: &crate::markup::v1::Refusal,
        _: &crate::markup::CommandContext,
    ) -> Result<Option<crate::Any>, crate::angzarr_router::error::CodedError> {
        Ok(None)
    }
}

/// A ledger `Record` for `cover`'s stream, as a policy's business code
/// returns it: no ids, no handling.
fn record_for(cover: crate::v1::Cover) -> CommandBook {
    command(
        cover,
        crate::pack(&ledger::Record {
            source_sequence: 0,
            cents: 1,
        }),
    )
}

/// The kernel, not the router, holds a policy's commands to its one
/// destination (decision 14.42, C-0583): one outside it refuses the whole
/// answer as the policy's bug, naming both domains; nothing is delivered,
/// the trigger is dead-lettered, and the position stays.
#[test]
fn a_policy_command_outside_its_destination_is_refused_by_the_kernel() {
    let mut registry = registry();
    registry.policy_component(
        Unchecked {
            answer: |_| {
                vec![
                    record_for(ledger_cover()),
                    record_for(crate::v1::Cover {
                        domain: "shelf".to_string(),
                        ..ledger_cover()
                    }),
                ]
            },
        },
        "ledger",
    );
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let failure = Kernel::new(&registry, &log)
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream {
        error: StepError::Kernel(KernelError::Router(error)),
        ..
    } = &failure
    else {
        panic!("the policy's bug: {failure:?}");
    };
    assert_eq!(
        error.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_DESTINATION"
    );
    let extra = |key: &str| error.extras.get(key).map(String::as_str);
    assert_eq!(extra("expected_domain"), Some("ledger"));
    assert_eq!(extra("actual_domain"), Some("shelf"));
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        streams(&store),
        [cover("account", 1)],
        "nothing delivered, not even the command inside the destination"
    );
    assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
    let reason = crate::toys::wire(
        "COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_DESTINATION",
        &[("actual_domain", "shelf"), ("expected_domain", "ledger")],
    );
    let letter = crate::toys::one_letter(log.letters());
    crate::toys::assert_letter(
        &letter,
        crate::v1::ComponentKind::Policy,
        &reason,
        crate::toys::processing_failed(&reason, 0, false),
    );
    assert!(
        matches!(
            &letter.payload,
            Some(crate::v1::angzarr_dead_letter::Payload::RejectedEvents(book))
                if book.cover == Some(cover("account", 1))
        ),
        "the trigger: {letter:?}"
    );
    assert_eq!(position(&store, "unchecked", &cover("account", 1)), 0);
}

/// What the kernel refuses `answer`'s commands with, as a policy of the
/// account's opening, nothing delivered.
fn unchecked_refusal(
    answer: fn(&crate::v1::EventBook) -> Vec<CommandBook>,
) -> crate::angzarr_router::error::CodedError {
    let mut registry = registry();
    registry.policy_component(Unchecked { answer }, "ledger");
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let failure = Kernel::new(&registry, &log)
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .expect("the policy's bug fails its step");
    assert_eq!(streams(&store), [cover("account", 1)], "nothing delivered");
    let error = match failure {
        Failure::Downstream {
            error: StepError::Kernel(KernelError::Router(error)),
            ..
        } => error,
        other => panic!("the policy's bug: {other:?}"),
    };
    // The trigger dead-lettered under the refusal's key and args, as
    // answered: processing_failed, a fatal bug (decision 14.48).
    let reason = crate::v1::Diagnostic {
        code: error.code.clone(),
        args: error.extras.clone().into_iter().collect(),
    };
    crate::toys::assert_letter(
        &crate::toys::one_letter(log.letters()),
        crate::v1::ComponentKind::Policy,
        &reason,
        crate::toys::processing_failed(&reason, 0, false),
    );
    error
}

/// A policy's command that carries Handling, which a reaction command
/// never may, refuses the whole answer as the policy's bug,
/// COORDINATOR_REASON_POLICY_COMMAND_WITH_HANDLING naming its domain and
/// type (decision 14.48): never delivered with its handling dropped.
#[test]
fn a_policys_command_with_handling_is_refused_by_the_kernel() {
    use crate::v1::{ConflictResolution, Handling};
    let sequenced = unchecked_refusal(|_| {
        let mut book = record_for(ledger_cover());
        book.handling = Some(Handling {
            expected_sequence: Some(0),
            ..Handling::default()
        });
        vec![book]
    });
    let resolved = unchecked_refusal(|_| {
        let mut book = record_for(ledger_cover());
        book.handling = Some(Handling {
            conflict_resolution: ConflictResolution::Strict as i32,
            ..Handling::default()
        });
        vec![book]
    });
    for refused in [sequenced, resolved] {
        assert_eq!(
            refused.code,
            "COORDINATOR_REASON_POLICY_COMMAND_WITH_HANDLING"
        );
        assert_eq!(
            refused.extras.get("domain").map(String::as_str),
            Some("ledger")
        );
        assert_eq!(
            refused.extras.get("command_type").map(String::as_str),
            Some("toy.ledger.cmd.v1.Record")
        );
        assert_eq!(
            refused.grpc,
            crate::angzarr_router::error::GrpcCode::Internal
        );
    }
    // An empty Handling is unset, and fine.
    let mut registry = registry();
    registry.policy_component(
        Unchecked {
            answer: |_| {
                let mut book = record_for(ledger_cover());
                book.handling = Some(crate::v1::Handling::default());
                vec![book]
            },
        },
        "ledger",
    );
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    Kernel::new(&registry, &log)
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .expect("delivered");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// The kernel holds each command to its trigger's edition, a named root and
/// the trigger's correlation (decision 14.42), each a refusal of the whole
/// answer as the policy's bug.
#[test]
fn a_policys_commands_are_held_to_their_trigger_by_the_kernel() {
    let outside = unchecked_refusal(|_| {
        vec![record_for(crate::v1::Cover {
            edition: Some("what-if".to_string()),
            ..ledger_cover()
        })]
    });
    assert_eq!(
        outside.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_EDITION"
    );
    assert_eq!(
        outside.extras.get("actual_edition").map(String::as_str),
        Some("what-if")
    );
    assert_eq!(
        outside.extras.get("expected_edition").map(String::as_str),
        Some("")
    );

    let rootless = unchecked_refusal(|_| {
        vec![record_for(crate::v1::Cover {
            root: None,
            ..ledger_cover()
        })]
    });
    assert_eq!(rootless.code, "COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT");
    assert_eq!(
        rootless.extras.get("domain").map(String::as_str),
        Some("ledger")
    );
    assert_eq!(
        rootless.extras.get("command_type").map(String::as_str),
        Some("toy.ledger.cmd.v1.Record")
    );
    let empty_root = unchecked_refusal(|_| {
        vec![record_for(crate::v1::Cover {
            root: Some(crate::v1::Uuid {
                value: Vec::new().into(),
            }),
            ..ledger_cover()
        })]
    });
    assert_eq!(empty_root.code, "COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT");

    let other = unchecked_refusal(|_| {
        let mut book = record_for(ledger_cover());
        book.page.as_mut().unwrap().correlation_id = Some(crate::toys::correlation(99));
        vec![book]
    });
    assert_eq!(
        other.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION"
    );
}

/// A command whose edition, root and correlation are its trigger's (or an
/// unset edition and correlation, stamped with the trigger's) is delivered.
#[test]
fn a_policys_commands_in_their_triggers_edition_and_correlation_are_delivered() {
    let mut registry = registry();
    registry.policy_component(
        Unchecked {
            answer: |source| {
                let trigger = source.pages[0]
                    .meta
                    .as_ref()
                    .and_then(|m| m.correlation_id.clone());
                let mut named = record_for(ledger_cover());
                named.page.as_mut().unwrap().correlation_id = trigger;
                vec![record_for(ledger_cover()), named]
            },
        },
        "ledger",
    );
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    Kernel::new(&registry, &log)
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .unwrap();
    assert_eq!(
        pages(&store, &ledger_cover()).len(),
        2,
        "both delivered: {:?}",
        log.lines()
    );
    assert_eq!(position(&store, "unchecked", &cover("account", 1)), 1);
}

/// A policy's reaction that is not a bare command refuses the whole answer
/// as the policy's bug, keyed as the spec spells it, naming its domain and
/// type (decision 14.48, amended): a command id or causation other than
/// the one its coordinator stamps,
/// `COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY` with the `field`; a
/// Fact, `COORDINATOR_REASON_POLICY_COMMAND_IS_FACT`; a command carrying
/// `fact_origin`, `COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN`. Never
/// delivered with the field dropped, nor a fact delivered as a command.
#[test]
fn a_policys_reaction_that_is_not_a_bare_command_is_refused_by_the_kernel() {
    use crate::v1::{command_page::Body, Fact, FactOrigin};
    let named = |refused: &crate::angzarr_router::error::CodedError, code: &str| {
        assert_eq!(refused.code, code, "{refused:?}");
        assert_eq!(
            refused.extras.get("domain").map(String::as_str),
            Some("ledger"),
            "{refused:?}"
        );
        assert_eq!(
            refused.extras.get("command_type").map(String::as_str),
            Some("toy.ledger.cmd.v1.Record"),
            "{refused:?}"
        );
        assert_eq!(
            refused.grpc,
            crate::angzarr_router::error::GrpcCode::Internal
        );
    };
    let field =
        |refused: &crate::angzarr_router::error::CodedError| refused.extras.get("field").cloned();

    let other_id = unchecked_refusal(|_| {
        let mut book = record_for(ledger_cover());
        book.page.as_mut().unwrap().command_id = Some(crate::toys::command_id(77));
        vec![book]
    });
    named(
        &other_id,
        "COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY",
    );
    assert_eq!(field(&other_id).as_deref(), Some("command_id"));

    let other_cause = unchecked_refusal(|source| {
        let mut book = record_for(ledger_cover());
        book.page.as_mut().unwrap().causation_id = Some(EventRef {
            stream: source.cover.clone(),
            sequence: source.pages[0].sequence + 9,
        });
        vec![book]
    });
    named(
        &other_cause,
        "COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY",
    );
    assert_eq!(field(&other_cause).as_deref(), Some("causation_id"));

    let is_fact = unchecked_refusal(|_| {
        let mut book = record_for(ledger_cover());
        let page = book.page.as_mut().unwrap();
        let Some(Body::Command(command)) = page.body.take() else {
            unreachable!()
        };
        page.body = Some(Body::Fact(Fact {
            payload: command.payload,
        }));
        page.fact_origin = Some(FactOrigin {
            external_id: "pi_1".to_string(),
            ..FactOrigin::default()
        });
        vec![book]
    });
    named(&is_fact, "COORDINATOR_REASON_POLICY_COMMAND_IS_FACT");
    assert_eq!(field(&is_fact), None);

    let origin = unchecked_refusal(|_| {
        let mut book = record_for(ledger_cover());
        book.page.as_mut().unwrap().fact_origin = Some(FactOrigin {
            external_id: "pi_1".to_string(),
            ..FactOrigin::default()
        });
        vec![book]
    });
    named(&origin, "COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN");
    assert_eq!(field(&origin), None);
}

/// A policy's commands naming exactly the command id and causation their
/// coordinator stamps (decision 14.48, amended: no id that DIFFERS from
/// the stamp) are delivered, each by its own index in the answer.
#[test]
fn a_policys_commands_naming_their_stamped_identity_are_delivered() {
    let mut registry = registry();
    registry.policy_component(
        Unchecked {
            answer: |source| {
                let trigger = EventRef {
                    stream: source.cover.clone(),
                    sequence: source.pages[0].sequence,
                };
                (0..2)
                    .map(|index| {
                        let mut book = record_for(ledger_cover());
                        let page = book.page.as_mut().unwrap();
                        page.command_id = Some(
                            crate::ids::policy_command_id(&trigger, "unchecked", index).unwrap(),
                        );
                        page.causation_id = Some(trigger.clone());
                        book
                    })
                    .collect()
            },
        },
        "ledger",
    );
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    Kernel::new(&registry, &log)
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .expect("delivered");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    assert_eq!(pages(&store, &ledger_cover()).len(), 2);

    // The first command's id at the second's index is another's: refused.
    let swapped = unchecked_refusal(|source| {
        let trigger = EventRef {
            stream: source.cover.clone(),
            sequence: source.pages[0].sequence,
        };
        let id = crate::ids::policy_command_id(&trigger, "unchecked", 0).unwrap();
        (0..2)
            .map(|_| {
                let mut book = record_for(ledger_cover());
                book.page.as_mut().unwrap().command_id = Some(id.clone());
                book
            })
            .collect()
    });
    assert_eq!(
        swapped.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY"
    );
}

/// The account's router table behind a handler recording every book it is
/// handed.
struct Recorded {
    inner: Box<dyn crate::angzarr_router::router::DynCommandHandler>,
    books: std::sync::Arc<std::sync::Mutex<Vec<CommandBook>>>,
}

impl crate::angzarr_router::router::CommandHandler for Recorded {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn command_types(&self) -> Vec<String> {
        self.inner.command_types()
    }
    async fn dispatch(
        &self,
        req: &crate::angzarr_router::pb::ContextualCommand,
    ) -> Result<
        crate::angzarr_router::pb::CommandHandlerResponse,
        crate::angzarr_router::error::CodedError,
    > {
        self.books
            .lock()
            .unwrap()
            .push(req.command.clone().unwrap_or_default());
        self.inner.dispatch(req).await
    }
}

/// With no translation, the kernel builds the pass-through `CommandRefused`
/// itself and addresses it (decision 14.42, C-0613): to the exact stream of
/// the triggering event, under the refused command's id, correlation and
/// causation, its fact origin the refused command's id.
#[test]
fn no_translation_makes_the_kernel_build_the_pass_through_fact() {
    use crate::markup::v1::CommandRefused;
    use crate::v1::command_page::Body;
    let books = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = crate::Registry::new();
    let table = crate::route::table::<Account>(
        std::sync::Arc::new(crate::toys::account::AccountState::default),
        false,
    );
    let inner = crate::toys::handlers::<Account>()(table);
    registry
        .command_handler(
            "account",
            Recorded {
                inner: Box::new(inner),
                books: std::sync::Arc::clone(&books),
            },
            crate::toys::descriptor_of::<Account>(crate::v1::ComponentKind::Aggregate, "v2", &[]),
        )
        .unwrap();
    crate::toys::register::<Ledger>(
        &mut registry,
        crate::Upcaster::new("ledger-upcaster", "ledger"),
    )
    .unwrap();
    registry.policy_component(
        Unchecked {
            answer: |source| {
                let deposited = source.pages[0]
                    .payload
                    .as_ref()
                    .is_some_and(|p| p.type_url.ends_with("Deposited"));
                match deposited {
                    true => {
                        let mut book = record_for(ledger_cover());
                        book.page.as_mut().unwrap().body =
                            Some(Body::Command(crate::v1::Command {
                                payload: Some(crate::pack(&ledger::Record {
                                    source_sequence: 1,
                                    cents: LEDGER_LIMIT + 1,
                                })),
                            }));
                        vec![book]
                    }
                    false => Vec::new(),
                }
            },
        },
        "ledger",
    );
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_retryable(crate::toys::no_retries());
    let store = MemoryStore::new();
    let account = cover("account", 1);
    for book in [
        command(account.clone(), AccountCmd::Open.to_any()).id(1),
        deposit(2, 5),
    ] {
        let _ = kernel.handle_any(&store, book, &at(30), &mut ()).ready();
    }

    let books = books.lock().unwrap();
    let fact = books.last().expect("the refusal fact reached the account");
    assert_eq!(fact.cover, Some(account.clone()), "the trigger's stream");
    let page = fact.page.as_ref().unwrap();
    let Some(Body::Fact(f)) = &page.body else {
        panic!("a fact: {page:?}");
    };
    let payload = f.payload.as_ref().unwrap();
    assert_eq!(
        crate::type_name(&payload.type_url),
        "io.angzarr.v1.CommandRefused"
    );
    let refused = <CommandRefused as prost::Message>::decode(&payload.value[..]).unwrap();
    let trigger = EventRef {
        stream: Some(account.clone()),
        sequence: 1,
    };
    assert_eq!(refused.trigger, Some(trigger.clone()));
    let command = refused.command.expect("the refused command");
    assert_eq!(command.cover, Some(ledger_cover()));
    let refused_id = ids::policy_command_id(&trigger, "unchecked", 0).unwrap();
    assert_eq!(
        command.page.as_ref().and_then(|p| p.command_id.clone()),
        Some(refused_id.clone())
    );
    assert_eq!(
        page.command_id,
        Some(refused_id.clone()),
        "the refused command's id"
    );
    assert_eq!(
        page.causation_id,
        Some(trigger),
        "the refused command's causation"
    );
    assert_eq!(
        page.fact_origin.as_ref().map(|o| o.external_id.clone()),
        Some(refused_id.to_string())
    );
}

/// The kernel's pass-through refusal fact (decision 14.42): the refused
/// command, its typed refusal still packed (none for an untyped
/// rejection), and the triggering event as the trigger.
#[test]
fn a_refusal_passes_through_as_command_refused() {
    use crate::v1::{refusal_response, CommandRefused, Cover, Diagnostic, RefusalResponse};
    use prost::Message;
    let trigger = Cover {
        domain: "table".to_string(),
        ..Cover::default()
    };
    let command = CommandBook {
        cover: Some(Cover {
            domain: "wallet".to_string(),
            ..Cover::default()
        }),
        ..CommandBook::default()
    };
    let typed = prost_types::Any {
        type_url: "/test.wallet.v1.Insufficient".to_string(),
        value: vec![8, 1],
    };
    let refusal = |reason| crate::markup::v1::Refusal {
        event: Some(crate::v1::EventBook {
            cover: Some(trigger.clone()),
            pages: vec![crate::v1::EventPage {
                sequence: 4,
                ..Default::default()
            }],
            ..Default::default()
        }),
        command: Some(command.clone()),
        response: Some(RefusalResponse { reason }),
    };
    let fact = super::passed_through(&refusal(Some(refusal_response::Reason::Refusal(
        typed.clone(),
    ))));
    assert_eq!(fact.type_url, "/io.angzarr.v1.CommandRefused");
    let decoded = CommandRefused::decode(fact.value.as_slice()).unwrap();
    assert_eq!(decoded.command, Some(command.clone()));
    assert_eq!(decoded.refusal, Some(typed));
    assert_eq!(
        decoded.trigger,
        Some(EventRef {
            stream: Some(trigger.clone()),
            sequence: 4,
        })
    );
    let untyped = super::passed_through(&refusal(Some(refusal_response::Reason::Diagnostic(
        Diagnostic::default(),
    ))));
    let decoded = CommandRefused::decode(untyped.value.as_slice()).unwrap();
    assert_eq!(decoded.refusal, None, "an untyped rejection packs none");
}

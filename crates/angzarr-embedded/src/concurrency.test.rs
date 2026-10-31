//! Same-stream concurrency is optimistic (red, design-by-test; the owner's
//! ruling of 2026-10-30): two commands to one stream at once both load and
//! both decide; the store takes the first write and refuses the second
//! (`StoreError::Conflict`). What the loser comes to depends on whether it
//! said what it expected:
//!
//! - **No expected sequence** (`CommandBook.handling.expected_sequence`
//!   unset): the kernel stamped the head it loaded, so the loss is the
//!   kernel's own race. It re-loads and decides again on the new head,
//!   under the retry policy, and succeeds.
//! - **An expected sequence**: the caller decided against a head that is
//!   gone. The loss is the same condition as the early check's stale
//!   sequence (`KernelError::Stale`, the stream already past it at load),
//!   and gets the same answer (the owner's ruling, 2026-10-30): ABORTED,
//!   class BusinessImmediate (decision 14.37: a direct caller gets it at
//!   once; a policy step is run again under the retry cap), under
//!   `COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE`, args `expected` and
//!   `actual`.
//!
//! Each test forces the race: both commands' handlers wait at a barrier
//! that only the two of them together can pass, so both have loaded the
//! same head before either writes. No sleep decides the outcome.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};

use angzarr_router::error::{CodedError, GrpcCode};
use angzarr_router::pb::CommandHandlerResponse;
use angzarr_router::router::CommandHandler;

use crate::toys::prelude::*;
use crate::toys::{at, command, command_id, cover};
use crate::v1::{
    CommandBook, ComponentDescriptor, ComponentKind, ConflictResolution, ContextualCommand,
    EventBook, EventPage, Handling, StateDescriptor, StateOptions,
};
use crate::{
    class, Any, Class, Failure, Handled, Kernel, KernelError, MemoryDeadLetters, Recovery,
    Registry, Rejected,
};

/// The reason key a stale expected sequence is answered with: the full
/// name of `CoordinatorReason::ExpectedSequenceStale` (decision 14.45).
fn expected_sequence_stale() -> &'static str {
    crate::reasons::CoordinatorReason::ExpectedSequenceStale.as_str_name()
}

/// `failure` is the coordinator's stale expected sequence for stream
/// `cover("till", 1)`, `expected` against `actual`: the kernel's
/// `Stale`, answered on the wire as ABORTED (BusinessImmediate) under
/// [`expected_sequence_stale`], its sequences as args; for a direct
/// caller, a retry (re-read and re-issue).
fn assert_stale(failure: &Failure<Rejected>, expected: u64, actual: u64) {
    assert_eq!(
        failure,
        &Failure::Kernel(KernelError::Stale {
            stream: cover("till", 1),
            expected,
            actual,
        })
    );
    let Failure::Kernel(error) = failure else {
        unreachable!()
    };
    let coded = error.as_coded().expect("one of the coordinator's answers");
    assert_eq!(coded.code, expected_sequence_stale());
    assert_eq!(coded.grpc, GrpcCode::Aborted);
    assert_eq!(class(coded.grpc as i32), Class::BusinessImmediate);
    assert_eq!(
        coded.extras,
        BTreeMap::from([
            ("actual".to_string(), actual.to_string()),
            ("expected".to_string(), expected.to_string()),
        ])
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
}

/// How many of the first dispatches meet at the barrier: the two racing
/// commands' first decisions. Any later dispatch (a retry) passes.
const RACERS: usize = 2;

/// An aggregate, `till`, whose first [`RACERS`] dispatches wait for each
/// other at a barrier: both have loaded before either decides. It records
/// how many events each dispatch saw, and answers one `Rung` event.
struct Racing {
    barrier: Barrier,
    dispatched: AtomicUsize,
    /// The length of the stream each dispatch was handed, in the order
    /// they came out of the barrier (or, past it, entered).
    saw: Mutex<Vec<usize>>,
}

impl Racing {
    fn new() -> Racing {
        Racing {
            barrier: Barrier::new(RACERS),
            dispatched: AtomicUsize::new(0),
            saw: Mutex::new(Vec::new()),
        }
    }
}

impl CommandHandler for &'static Racing {
    fn name(&self) -> &str {
        "Till"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.till.v1.Ring".to_string()]
    }

    async fn dispatch(
        &self,
        command: &ContextualCommand,
    ) -> Result<CommandHandlerResponse, CodedError> {
        if self.dispatched.fetch_add(1, Ordering::SeqCst) < RACERS {
            self.barrier.wait();
        }
        let seen = command.events.as_ref().map_or(0, |e| e.pages.len());
        self.saw.lock().unwrap().push(seen);
        Ok(CommandHandlerResponse {
            conflict: None,
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any("/toy.till.v1.Rung")),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: "till-1".to_string(),
        })
    }
}

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

fn till_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Till".to_string(),
        kind: ComponentKind::Aggregate as i32,
        rules_version: "till-1".to_string(),
        state: Some(StateDescriptor {
            type_url: "/toy.till.v1.State".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }),
        ..ComponentDescriptor::default()
    }
}

/// Command `n` to the till's stream 1, expecting `expected` if given.
fn ring(n: u128, expected: Option<u64>) -> CommandBook {
    let mut book = command(cover("till", 1), any("/toy.till.v1.Ring"));
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(command_id(n));
    }
    book.handling = expected.map(|expected| Handling {
        expected_sequence: Some(expected),
        conflict_resolution: ConflictResolution::Strict as i32,
    });
    book
}

/// `books` handled at once, each on a thread of its own with a runtime
/// of its own, through one kernel over one store: their results, in the
/// order given.
fn race(
    registry: &Registry,
    log: &MemoryDeadLetters,
    store: &MemoryStore,
    books: [CommandBook; 2],
) -> Vec<Result<Handled, Failure<Rejected>>> {
    let kernel = Kernel::new(registry, log);
    std::thread::scope(|scope| {
        let racers: Vec<_> = books
            .into_iter()
            .enumerate()
            .map(|(k, book)| {
                let kernel = &kernel;
                scope.spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap()
                        .block_on(kernel.handle_any(store, book, &at(k as i64 + 1), &mut ()))
                })
            })
            .collect();
        racers.into_iter().map(|r| r.join().unwrap()).collect()
    })
}

fn registry_of(racing: &'static Racing) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("till", racing, till_descriptor())
        .unwrap();
    registry
}

/// Neither command set an expected sequence: both decide on the empty
/// stream, one write wins, and the loser is re-loaded and decided again on
/// the new head, so both are applied. The stream holds both events,
/// gapless; the loser's handler ran twice, the second time on the
/// winner's event.
#[test]
fn a_lost_race_without_an_expected_sequence_is_retried_on_the_new_head() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let results = race(&registry, &log, &store, [ring(1, None), ring(2, None)]);

    for (k, result) in results.iter().enumerate() {
        assert!(
            matches!(result, Ok(Handled::Applied(_))),
            "command {}: {:?}",
            k + 1,
            result.as_ref().err()
        );
    }
    let sequences: Vec<_> = pages(&store, &cover("till", 1))
        .iter()
        .map(|p| p.sequence)
        .collect();
    assert_eq!(sequences, [0, 1]);
    assert_eq!(
        *racing.saw.lock().unwrap(),
        [0, 0, 1],
        "both raced on the empty stream; the loser decided again on the winner's event"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// Both commands expected the empty stream (sequence 0): both pass the
/// check, both decide, one write wins. The loser decided against a head
/// that is gone: the coordinator's stale expected sequence, 0 against 1,
/// returned to this direct caller at once (its handler ran once, it is not
/// re-decided) and never dead-lettered. The stream holds the winner's
/// event only.
#[test]
fn a_lost_race_with_an_expected_sequence_is_a_stale_expected_sequence() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let results = race(
        &registry,
        &log,
        &store,
        [ring(1, Some(0)), ring(2, Some(0))],
    );

    let applied = results
        .iter()
        .filter(|r| matches!(r, Ok(Handled::Applied(_))))
        .count();
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(applied, 1, "one write wins: {errors:?}");
    assert_stale(errors[0], 0, 1);
    assert_eq!(
        racing.dispatched.load(Ordering::SeqCst),
        RACERS,
        "the loser is not re-decided"
    );
    assert_eq!(pages(&store, &cover("till", 1)).len(), 1);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// The early check: a command expecting sequence 0 of a stream already at
/// 1 is refused before its handler runs, with the same answer as a lost
/// race: the coordinator's stale expected sequence, 0 against 1.
#[test]
fn an_expected_sequence_already_passed_is_a_stale_expected_sequence() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    // Past the barrier: no race here, every dispatch passes.
    racing.dispatched.store(RACERS, Ordering::SeqCst);
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let kernel = Kernel::new(&registry, &log);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let first = runtime.block_on(kernel.handle_any(&store, ring(1, None), &at(1), &mut ()));
    assert!(
        matches!(first, Ok(Handled::Applied(_))),
        "{:?}",
        first.err()
    );

    let late = runtime
        .block_on(kernel.handle_any(&store, ring(2, Some(0)), &at(2), &mut ()))
        .err()
        .expect("stale");
    assert_stale(&late, 0, 1);
    assert_eq!(
        racing.dispatched.load(Ordering::SeqCst),
        RACERS + 1,
        "refused before its handler"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

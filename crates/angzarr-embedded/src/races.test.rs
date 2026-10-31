//! The coordinator's conflict-resolution contract at load and at the
//! write (angzarr-project `features/coordinator-contract/conflict_resolution.feature`;
//! decisions 14.45, 14.46, 14.47), on an `order` stream holding events 0
//! to 2 (its head at 3), in every resolution its state supports (STRICT,
//! AGGREGATE_HANDLES, COMMUTATIVE).
//!
//! - **Ahead** (C-0727): an expected sequence past the head is malformed
//!   under every resolution, refused INVALID_ARGUMENT before business code.
//! - **A race lost at the write** (C-0718, C-0719, C-0722 to C-0726):
//!   another write to the same stream lands between a command's load and
//!   its append. STRICT with an expected sequence is stale; AGGREGATE_HANDLES
//!   and COMMUTATIVE re-deliver the widened window with a fresh nonce,
//!   under the retry cap; a command stamped with the head (no expected
//!   sequence, or a fact) is re-run on the new head, under the race re-run
//!   cap, with backoff between runs. At a cap the command is dead-lettered,
//!   `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED` (INTERNAL), and a fact is
//!   parked.
//!
//! Each race is forced, never timed: the business code itself lands the
//! other write on the stream, through the store, while it decides, so the
//! write lands after the delivery and before the command's append, every
//! time.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;

use angzarr_router::error::{CodedError, GrpcCode};
use angzarr_router::pb::{CommandHandlerResponse, ConflictReport};
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::CommandHandler;

use crate::reasons::CoordinatorReason;
use crate::toys::prelude::*;
use crate::toys::{at, command, cover, fact, Edit};
use crate::v1::{
    command_page, Command, CommandBook, CommandPage, ComponentDescriptor, ComponentKind,
    ConflictResolution, ContextualCommand, Cover, DeliveryOutcome, EventBook, EventPage,
    StateDescriptor, StateOptions,
};
use crate::{
    class, Any, Class, Failure, Handled, Kernel, KernelError, MemoryDeadLetters, OnRetryable,
    Recovery, Registry, Rejected, StepError, Wait,
};

const RESOLUTIONS: [ConflictResolution; 3] = [
    ConflictResolution::Strict,
    ConflictResolution::AggregateHandles,
    ConflictResolution::Commutative,
];

const ITEM_ADDED: &str = "/toy.order.v1.ItemAdded";
const SHIPMENT_DISPATCHED: &str = "/toy.order.v1.ShipmentDispatched";
const NOTE_ADDED: &str = "/toy.order.v1.OrderNoteAdded";

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// The order stream every test runs on.
fn order() -> Cover {
    cover("order", 1)
}

/// What business code was handed on one delivery: the expected sequence,
/// the head (the window runs from the one to the other) and the nonce.
#[derive(Clone, Debug, PartialEq)]
struct Seen {
    expected: u64,
    head: u64,
    nonce: Vec<u8>,
}

/// What business code reports with its events.
#[derive(Clone, Copy, Debug)]
enum Reported {
    /// No conflict report.
    Nothing,
    /// AGGREGATE_HANDLES: the nonce of this delivery as its resolved one.
    ThisNonce,
    /// AGGREGATE_HANDLES: the nonce of the first delivery.
    FirstNonce,
    /// COMMUTATIVE: the units the window changed and the command changes.
    Units(&'static [&'static str], &'static [&'static str]),
}

/// One delivery's script: what business code reports, and whether, while
/// it decides, another write lands on the order stream.
#[derive(Clone, Copy, Debug)]
struct Delivery {
    reported: Reported,
    races: bool,
}

impl Delivery {
    /// No report, no race.
    fn plain() -> Delivery {
        Delivery::alone(Reported::Nothing)
    }

    /// Reporting `reported`, and losing the race to another write.
    fn racing(reported: Reported) -> Delivery {
        Delivery {
            reported,
            races: true,
        }
    }

    /// Reporting `reported`, and no other write.
    fn alone(reported: Reported) -> Delivery {
        Delivery {
            reported,
            races: false,
        }
    }
}

/// The `order` aggregate, hand-written as the router's `CommandHandler`:
/// every delivery it is handed, in order, answered with one `ItemAdded`
/// (a fact, with one `ShipmentDispatched`) as its script says, the last
/// step of the script repeated past its end.
struct Orders {
    /// Where another write is landed: the store the kernel runs on.
    store: Option<MemoryStore>,
    script: Vec<Delivery>,
    seen: Mutex<Vec<Seen>>,
}

impl Orders {
    fn scripted(store: Option<&MemoryStore>, script: Vec<Delivery>) -> &'static Orders {
        Box::leak(Box::new(Orders {
            store: store.cloned(),
            script,
            seen: Mutex::new(Vec::new()),
        }))
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// Another write to the order stream: a note at its head.
    async fn land_another_write(&self) {
        let store = self.store.as_ref().expect("a racing script has the store");
        let head = store
            .read(sref(&order()), 0, None)
            .await
            .unwrap()
            .next_sequence;
        store
            .write(Write::append(sref(&order()), head, &[note(head)]))
            .await
            .unwrap();
    }
}

impl CommandHandler for &'static Orders {
    fn name(&self) -> &str {
        "Order"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.order.v1.AddItem".to_string()]
    }

    async fn dispatch(
        &self,
        command: &ContextualCommand,
    ) -> Result<CommandHandlerResponse, CodedError> {
        let head = command.events.as_ref().map_or(0, |e| e.next_sequence);
        let book = command.command.as_ref();
        let expected = book
            .and_then(|book| book.handling.as_ref())
            .and_then(|handling| handling.expected_sequence)
            .unwrap_or(head);
        let nonce = command.conflict_nonce.to_vec();
        let delivery = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(Seen {
                expected,
                head,
                nonce: nonce.clone(),
            });
            self.script[(seen.len() - 1).min(self.script.len() - 1)]
        };
        if delivery.races {
            self.land_another_write().await;
        }
        let units = |units: &[&str]| units.iter().map(|u| u.to_string()).collect();
        let conflict = match delivery.reported {
            Reported::Nothing => None,
            Reported::ThisNonce => Some(ConflictReport {
                resolved_nonce: nonce,
                ..ConflictReport::default()
            }),
            Reported::FirstNonce => Some(ConflictReport {
                resolved_nonce: self.seen()[0].nonce.clone(),
                ..ConflictReport::default()
            }),
            Reported::Units(window, changed) => Some(ConflictReport {
                window_units: units(window),
                command_units: units(changed),
                ..ConflictReport::default()
            }),
        };
        let is_fact = book
            .and_then(|book| book.page.as_ref())
            .is_some_and(|page| matches!(page.body, Some(command_page::Body::Fact(_))));
        Ok(CommandHandlerResponse {
            conflict,
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any(match is_fact {
                        true => SHIPMENT_DISPATCHED,
                        false => ITEM_ADDED,
                    })),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: "order-1".to_string(),
        })
    }
}

fn order_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Order".to_string(),
        kind: ComponentKind::Aggregate as i32,
        rules_version: "order-1".to_string(),
        state: Some(StateDescriptor {
            type_url: "/toy.order.v1.State".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: RESOLUTIONS.iter().map(|r| *r as i32).collect(),
            }),
        }),
        ..ComponentDescriptor::default()
    }
}

fn registry_of(orders: &'static Orders) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("order", orders, order_descriptor())
        .unwrap();
    registry
}

/// A note at `sequence` of the order stream: an event no command of the
/// test's wrote.
fn note(sequence: u64) -> EventPage {
    EventPage {
        sequence,
        payload: Some(any(NOTE_ADDED)),
        ..EventPage::default()
    }
}

/// The background: the order stream holds events 0 to 2, its head at 3.
fn store_at_three() -> MemoryStore {
    let store = MemoryStore::new();
    store
        .write(Write::append(
            sref(&order()),
            0,
            &[note(0), note(1), note(2)],
        ))
        .ready()
        .unwrap();
    store
}

/// An AddItem to the order stream, command id `n`.
fn add_item(n: u128) -> CommandBook {
    command(order(), any("/toy.order.v1.AddItem")).id(n)
}

/// The order stream's payload types, in sequence order.
fn stored(store: &MemoryStore) -> Vec<String> {
    pages(store, &order())
        .into_iter()
        .map(|page| page.payload.unwrap_or_default().type_url)
        .collect()
}

/// The background's three notes, then `more` notes no command wrote.
fn notes(more: usize) -> Vec<String> {
    vec![NOTE_ADDED.to_string(); 3 + more]
}

/// `failure` is never answered the stale expected sequence, STRICT's key.
fn never_stale(failure: &Failure<Rejected>) {
    if let Failure::Kernel(error) = failure {
        if let Some(coded) = error.as_coded() {
            assert_ne!(
                coded.code,
                CoordinatorReason::ExpectedSequenceStale.as_str_name(),
                "{failure:?}"
            );
        }
    }
}

/// Waits that complete at once, kept; every jitter draw 0, so each is the
/// backoff itself.
#[derive(Default)]
struct Waits(Mutex<Vec<Duration>>);

impl Waits {
    fn waited(&self) -> Vec<Duration> {
        self.0.lock().unwrap().clone()
    }
}

impl Wait for Waits {
    fn wait(
        &self,
        backoff: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The kernel's backoffs before its first `n` re-runs, jitter drawn 0.
fn backoffs(n: u32) -> Vec<Duration> {
    let retry = OnRetryable::default();
    (0..n).map(|k| retry.backoff(k, 0.0)).collect()
}

/// A kernel retrying as `retry` says, waiting on `waits`.
fn kernel<'k>(
    registry: &'k Registry,
    log: &'k MemoryDeadLetters,
    waits: &'k Waits,
    retry: OnRetryable,
) -> Kernel<'k> {
    Kernel::new(registry, log)
        .on_retryable(retry)
        .wait_with(waits)
}

/// `failure` is the kernel giving up on the order stream at a cap,
/// `attempts` runs of business code against `cap`: INTERNAL,
/// `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED`, args `attempts` and `cap`, a
/// fatal bug's class (never retried by its caller), never the stale key.
fn assert_cap_exhausted(failure: &Failure<Rejected>, attempts: u32, cap: u32) {
    assert_eq!(
        failure,
        &Failure::Kernel(KernelError::RetryCapExhausted {
            stream: order(),
            attempts,
            cap,
        })
    );
    let Failure::Kernel(error) = failure else {
        unreachable!()
    };
    let coded = error.as_coded().expect("one of the coordinator's answers");
    assert_eq!(
        coded.code,
        CoordinatorReason::RetryCapExhausted.as_str_name()
    );
    assert_eq!(coded.grpc, GrpcCode::Internal);
    assert_eq!(class(coded.grpc as i32), Class::BugFatal);
    assert_eq!(
        coded.extras,
        BTreeMap::from([
            ("attempts".to_string(), attempts.to_string()),
            ("cap".to_string(), cap.to_string()),
        ])
    );
    assert_eq!(failure.recovery(), Recovery::Bug);
    never_stale(failure);
}

/// `log` holds one dead letter, the kernel giving up at a cap: its reason
/// the retry cap's key as the spec spells it,
/// `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED`, with `attempts` and `cap`, and
/// its one detail `processing_failed`, `retry_count` the retries (the
/// attempts after the first), transient (decision 14.48).
fn assert_dead_lettered_at_cap(log: &MemoryDeadLetters, attempts: u32, cap: u32) {
    use crate::toys::{assert_letter, one_letter, processing_failed, wire};
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    let line = &lines[0];
    assert!(line.starts_with(crate::DEAD_LETTER_PREFIX), "{line}");
    let reason = wire(
        "COORDINATOR_REASON_RETRY_CAP_EXHAUSTED",
        &[
            ("attempts", &attempts.to_string()),
            ("cap", &cap.to_string()),
        ],
    );
    let letter = one_letter(log.letters());
    // retry_count counts the retries, the runs after the first
    // (ProcessingFailedDetails.retry_count), not the attempts.
    assert_letter(
        &letter,
        crate::v1::ComponentKind::Aggregate,
        &reason,
        processing_failed(&reason, attempts - 1, true),
    );
    assert!(
        matches!(
            letter.payload,
            Some(crate::v1::angzarr_dead_letter::Payload::RejectedCommand(_))
        ),
        "{letter:?}"
    );
}

/// C-0727: a command whose expected sequence is ahead of the head (4 or 7,
/// the head at 3) is malformed under every resolution: refused
/// INVALID_ARGUMENT with `COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD`, args
/// `expected` and `head`, a final business refusal (never retried); business
/// code is not invoked, nothing is persisted, and it is never answered the
/// stale key.
#[test]
fn a_command_ahead_of_the_stream_is_malformed_under_every_resolution() {
    for resolution in RESOLUTIONS {
        for expected in [4, 7] {
            let orders = Orders::scripted(None, vec![Delivery::plain()]);
            let registry = registry_of(orders);
            let log = MemoryDeadLetters::new();
            let store = store_at_three();
            let failure = Kernel::new(&registry, &log)
                .handle_any(
                    &store,
                    add_item(1).expecting(expected, resolution),
                    &at(1),
                    &mut (),
                )
                .ready()
                .err()
                .expect("refused");
            let case = format!("{resolution:?} expecting {expected}");
            assert_eq!(
                failure,
                Failure::Kernel(KernelError::ExpectedSequenceAhead {
                    stream: order(),
                    expected,
                    head: 3,
                }),
                "{case}"
            );
            let Failure::Kernel(error) = &failure else {
                unreachable!()
            };
            let coded = error.as_coded().expect("one of the coordinator's answers");
            assert_eq!(
                coded.code,
                CoordinatorReason::ExpectedSequenceAhead.as_str_name(),
                "{case}"
            );
            assert_eq!(coded.grpc, GrpcCode::InvalidArgument, "{case}");
            assert_eq!(class(coded.grpc as i32), Class::BusinessFatal, "{case}");
            assert_eq!(
                coded.extras,
                BTreeMap::from([
                    ("expected".to_string(), expected.to_string()),
                    ("head".to_string(), "3".to_string()),
                ]),
                "{case}"
            );
            assert_eq!(failure.recovery(), Recovery::Invalid, "{case}");
            never_stale(&failure);
            assert!(orders.seen().is_empty(), "{case}: business code ran");
            assert_eq!(pages(&store, &order()).len(), 3, "{case}");
            assert!(log.lines().is_empty(), "{case}: {:?}", log.lines());
        }
    }
}

/// C-0718: under STRICT, a command expecting the head that loses the race
/// at its write is stale, expected 3 against 4, and is not run again.
#[test]
fn under_strict_a_race_lost_at_the_write_is_stale_and_not_run_again() {
    let store = store_at_three();
    let orders = Orders::scripted(Some(&store), vec![Delivery::racing(Reported::Nothing)]);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let failure = kernel(&registry, &log, &waits, OnRetryable::default())
        .handle_any(
            &store,
            add_item(1).expecting(3, ConflictResolution::Strict),
            &at(1),
            &mut (),
        )
        .ready()
        .err()
        .expect("stale");
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Stale {
            stream: order(),
            expected: 3,
            actual: 4,
        })
    );
    assert_eq!(orders.seen().len(), 1, "not run again");
    assert_eq!(stored(&store), notes(1), "the command persisted nothing");
    assert!(waits.waited().is_empty());
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// C-0722: under AGGREGATE_HANDLES, a command expecting 1 resolves the
/// window it was delivered (1 to 3) with that delivery's nonce, and loses
/// the race at its write. The coordinator re-delivers the widened window
/// (1 to 4) with a fresh nonce, after the retry's backoff, and the nonce
/// the second delivery reports decides: its own is kept, at sequence 4;
/// the first delivery's, or none, is unresolved and persists nothing.
/// Never the stale key.
#[test]
fn under_aggregate_handles_a_race_lost_at_the_write_redelivers_the_widened_window() {
    for (second, kept) in [
        (Reported::ThisNonce, true),
        (Reported::FirstNonce, false),
        (Reported::Nothing, false),
    ] {
        let case = format!("second delivery reporting {second:?}");
        let store = store_at_three();
        let orders = Orders::scripted(
            Some(&store),
            vec![
                Delivery::racing(Reported::ThisNonce),
                Delivery::alone(second),
            ],
        );
        let registry = registry_of(orders);
        let log = MemoryDeadLetters::new();
        let waits = Waits::default();
        let handled = kernel(&registry, &log, &waits, OnRetryable::default())
            .handle_any(
                &store,
                add_item(1).expecting(1, ConflictResolution::AggregateHandles),
                &at(1),
                &mut (),
            )
            .ready();

        let seen = orders.seen();
        assert_eq!(seen.len(), 2, "{case}: invoked twice");
        assert_eq!((seen[0].expected, seen[0].head), (1, 3), "{case}");
        assert_eq!(
            (seen[1].expected, seen[1].head),
            (1, 4),
            "{case}: the window widened to the new head"
        );
        assert!(!seen[1].nonce.is_empty(), "{case}");
        assert_ne!(seen[1].nonce, seen[0].nonce, "{case}: a fresh nonce");
        assert_eq!(waits.waited(), backoffs(1), "{case}");
        match kept {
            true => {
                assert!(
                    matches!(handled, Ok(Handled::Applied(_))),
                    "{case}: {:?}",
                    handled.err()
                );
                let mut expected = notes(1);
                expected.push(ITEM_ADDED.to_string());
                assert_eq!(stored(&store), expected, "{case}: ItemAdded at 4");
            }
            false => {
                let failure = handled.err().expect("unresolved");
                assert_eq!(
                    failure,
                    Failure::Kernel(KernelError::Unresolved {
                        stream: order(),
                        expected: 1,
                        actual: 4,
                    }),
                    "{case}"
                );
                never_stale(&failure);
                assert_eq!(stored(&store), notes(1), "{case}: nothing persisted");
            }
        }
    }
}

/// C-0723: under COMMUTATIVE, a command expecting 1 whose units do not
/// overlap the window's loses the race at its write; the coordinator
/// re-delivers the widened window with a fresh nonce, and unit overlap
/// decides on it: disjoint is kept, at sequence 4; overlapping persists
/// nothing. Never the stale key.
#[test]
fn under_commutative_a_race_lost_at_the_write_redelivers_the_widened_window() {
    const APPLE: &[&str] = &["items[apple]"];
    for (window, kept) in [
        (&["items[pear]", "items[kiwi]"][..], true),
        (&["items[pear]", "items[apple]"][..], false),
    ] {
        let case = format!("second window {window:?}");
        let store = store_at_three();
        let orders = Orders::scripted(
            Some(&store),
            vec![
                Delivery::racing(Reported::Units(&["items[pear]"], APPLE)),
                Delivery::alone(Reported::Units(window, APPLE)),
            ],
        );
        let registry = registry_of(orders);
        let log = MemoryDeadLetters::new();
        let waits = Waits::default();
        let handled = kernel(&registry, &log, &waits, OnRetryable::default())
            .handle_any(
                &store,
                add_item(1).expecting(1, ConflictResolution::Commutative),
                &at(1),
                &mut (),
            )
            .ready();

        let seen = orders.seen();
        assert_eq!(seen.len(), 2, "{case}: invoked twice");
        assert_eq!(
            (seen[1].expected, seen[1].head),
            (1, 4),
            "{case}: the window widened to the new head"
        );
        assert!(!seen[1].nonce.is_empty(), "{case}");
        assert_ne!(seen[1].nonce, seen[0].nonce, "{case}: a fresh nonce");
        match kept {
            true => {
                assert!(
                    matches!(handled, Ok(Handled::Applied(_))),
                    "{case}: {:?}",
                    handled.err()
                );
                let mut expected = notes(1);
                expected.push(ITEM_ADDED.to_string());
                assert_eq!(stored(&store), expected, "{case}: ItemAdded at 4");
            }
            false => {
                let failure = handled.err().expect("overlap");
                let Failure::Kernel(error @ KernelError::Overlap { .. }) = &failure else {
                    panic!("{case}: {failure:?}");
                };
                assert_eq!(
                    error.as_coded().map(|c| c.code),
                    Some(CoordinatorReason::ConflictOverlap.as_str_name().to_string()),
                    "{case}"
                );
                assert_eq!(stored(&store), notes(1), "{case}: nothing persisted");
            }
        }
    }
}

/// C-0724: with the retry cap reached after 3 deliveries (two retries), an
/// AGGREGATE_HANDLES or COMMUTATIVE command that accepts every window and
/// loses the race at every write is delivered 3 times, each a wider window
/// with a fresh nonce, then its caller is answered INTERNAL,
/// `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED`, attempts 3 and cap 3. Its
/// caller is waiting, so nothing is dead-lettered (owner ruling,
/// 2026-10-31: only work with no caller left is). It persists nothing, and
/// is never answered the stale key.
#[test]
fn a_window_lost_at_every_write_until_the_retry_cap_is_answered_given_up() {
    for (resolution, accepted) in [
        (ConflictResolution::AggregateHandles, Reported::ThisNonce),
        (
            ConflictResolution::Commutative,
            Reported::Units(&["items[pear]"], &["items[apple]"]),
        ),
    ] {
        let case = format!("{resolution:?}");
        let store = store_at_three();
        let orders = Orders::scripted(Some(&store), vec![Delivery::racing(accepted)]);
        let registry = registry_of(orders);
        let log = MemoryDeadLetters::new();
        let waits = Waits::default();
        let retry = OnRetryable {
            max_retries: 2,
            ..OnRetryable::default()
        };
        let failure = kernel(&registry, &log, &waits, retry)
            .handle_any(
                &store,
                add_item(1).expecting(1, resolution),
                &at(1),
                &mut (),
            )
            .ready()
            .err()
            .expect("given up at the cap");

        let seen = orders.seen();
        let windows: Vec<_> = seen.iter().map(|s| (s.expected, s.head)).collect();
        assert_eq!(windows, [(1, 3), (1, 4), (1, 5)], "{case}: each wider");
        let nonces: BTreeSet<_> = seen.iter().map(|s| s.nonce.clone()).collect();
        assert_eq!(nonces.len(), 3, "{case}: each nonce fresh");
        assert_cap_exhausted(&failure, 3, 3);
        assert!(log.lines().is_empty(), "{case}: {:?}", log.lines());
        assert_eq!(stored(&store), notes(3), "{case}: nothing persisted");
        assert_eq!(waits.waited(), backoffs(2), "{case}");
    }
}

/// C-0719: a command with no expected sequence that loses the race at its
/// write is run again on the new head, after a backoff, and lands at 4.
#[test]
fn a_stamped_command_that_loses_the_race_runs_again_on_the_new_head() {
    let store = store_at_three();
    let orders = Orders::scripted(
        Some(&store),
        vec![Delivery::racing(Reported::Nothing), Delivery::plain()],
    );
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let handled = kernel(&registry, &log, &waits, OnRetryable::default())
        .handle_any(&store, add_item(1), &at(1), &mut ())
        .ready();
    assert!(
        matches!(handled, Ok(Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    let heads: Vec<_> = orders.seen().iter().map(|s| s.head).collect();
    assert_eq!(heads, [3, 4], "the second run against the head at 4");
    let mut expected = notes(1);
    expected.push(ITEM_ADDED.to_string());
    assert_eq!(stored(&store), expected);
    assert_eq!(waits.waited(), backoffs(1));
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// The race re-run cap is its own (decision 14.46): with retrying off
/// (`max_retries` 0), a stamped command that loses the race still runs
/// again.
#[test]
fn a_stamped_command_runs_again_with_retrying_off() {
    let store = store_at_three();
    let orders = Orders::scripted(
        Some(&store),
        vec![Delivery::racing(Reported::Nothing), Delivery::plain()],
    );
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_retries: 0,
        ..OnRetryable::default()
    };
    let handled = kernel(&registry, &log, &waits, retry)
        .handle_any(&store, add_item(1), &at(1), &mut ())
        .ready();
    assert!(
        matches!(handled, Ok(Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    assert_eq!(orders.seen().len(), 2);
}

/// A race re-run cap of 0 runs a stamped command that loses the race at
/// its write no more: the lost race goes back to its caller at once as the
/// store's conflict, retryable, after one run of business code, with no
/// wait, nothing dead-lettered and nothing of its own persisted, for a
/// caller that owns its retry loop. Whatever `max_retries` says.
#[test]
fn with_no_race_reruns_a_lost_race_goes_back_as_the_stores_conflict() {
    for max_retries in [0, 3] {
        let store = store_at_three();
        let orders = Orders::scripted(
            Some(&store),
            vec![Delivery::racing(Reported::Nothing), Delivery::plain()],
        );
        let registry = registry_of(orders);
        let log = MemoryDeadLetters::new();
        let waits = Waits::default();
        let retry = OnRetryable {
            max_retries,
            max_race_runs: 0,
            ..OnRetryable::default()
        };
        let failure = kernel(&registry, &log, &waits, retry)
            .handle_any(&store, add_item(1), &at(1), &mut ())
            .ready()
            .err()
            .expect("the race was lost");
        assert!(
            matches!(
                failure,
                Failure::Kernel(KernelError::Store(crate::StoreError::Conflict { .. }))
            ),
            "max_retries {max_retries}: {failure:?}"
        );
        assert_eq!(
            failure.recovery(),
            Recovery::Retry,
            "max_retries {max_retries}"
        );
        assert_eq!(
            store_conflict(&failure),
            Some(conflict_answer()),
            "max_retries {max_retries}: keyed (14.51)"
        );
        assert_eq!(
            orders.seen().len(),
            1,
            "max_retries {max_retries}: run once"
        );
        assert!(
            waits.waited().is_empty(),
            "max_retries {max_retries}: no wait"
        );
        assert!(log.lines().is_empty(), "{:?}", log.lines());
        assert_eq!(stored(&store), notes(1), "max_retries {max_retries}");
    }
}

/// C-0725: with the race re-run cap at 3 runs and the retry cap higher (5
/// deliveries, never reached), a command with no expected sequence that
/// loses the race at every write runs 3 times, each against a newer head,
/// backing off between runs, then its caller is answered INTERNAL,
/// `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED`, attempts 3 and cap 3. Its
/// caller is waiting, so nothing is dead-lettered (owner ruling,
/// 2026-10-31). It persists nothing, and is never answered the stale key.
#[test]
fn a_stamped_command_losing_every_race_stops_at_the_race_rerun_cap() {
    let store = store_at_three();
    let orders = Orders::scripted(Some(&store), vec![Delivery::racing(Reported::Nothing)]);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_retries: 4,
        max_race_runs: 3,
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(&store, add_item(1), &at(1), &mut ())
        .ready()
        .err()
        .expect("given up at the cap");
    let heads: Vec<_> = orders.seen().iter().map(|s| s.head).collect();
    assert_eq!(heads, [3, 4, 5], "each run against a newer head");
    assert_cap_exhausted(&failure, 3, 3);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    assert_eq!(stored(&store), notes(3), "nothing persisted");
    assert_eq!(waits.waited(), backoffs(2), "backed off between runs");
}

/// Decision 14.46: the race re-run cap is 10 runs by default.
#[test]
fn the_race_rerun_cap_is_ten_runs_by_default() {
    assert_eq!(OnRetryable::default().max_race_runs, 10);
    let store = store_at_three();
    let orders = Orders::scripted(Some(&store), vec![Delivery::racing(Reported::Nothing)]);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let failure = kernel(&registry, &log, &waits, OnRetryable::default())
        .handle_any(&store, add_item(1), &at(1), &mut ())
        .ready()
        .err()
        .expect("given up at the cap");
    assert_eq!(orders.seen().len(), 10);
    assert_cap_exhausted(&failure, 10, 10);
    assert_eq!(waits.waited(), backoffs(9));
}

/// C-0726: a fact that loses the race at every write until the race
/// re-run cap (3 runs) runs 3 times, each against a newer head, then is
/// dead-lettered, persisting nothing, and its sender is answered OK,
/// PARKED, with no events: never refused, never the cap's key.
#[test]
fn a_fact_losing_every_race_until_the_cap_is_parked() {
    let store = store_at_three();
    let orders = Orders::scripted(Some(&store), vec![Delivery::racing(Reported::Nothing)]);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_race_runs: 3,
        ..OnRetryable::default()
    };
    let handled = kernel(&registry, &log, &waits, retry)
        .handle_any(
            &store,
            fact(order(), any(SHIPMENT_DISPATCHED), "shipment-1"),
            &at(1),
            &mut (),
        )
        .ready();
    let heads: Vec<_> = orders.seen().iter().map(|s| s.head).collect();
    assert_eq!(heads, [3, 4, 5], "each run against a newer head");
    let handled = handled.expect("a fact is never refused");
    assert!(matches!(handled, Handled::Parked));
    assert_eq!(
        handled.outcome().delivery,
        DeliveryOutcome::Parked as i32,
        "OK, PARKED"
    );
    assert!(handled.books().is_empty(), "no events");
    assert_dead_lettered_at_cap(&log, 3, 3);
    assert_eq!(stored(&store), notes(3), "nothing persisted");
}

/// A race re-run cap of 0 turns race re-runs off for a fact too: a fact
/// that loses the race at its write is not run again, not dead-lettered
/// and not parked; its sender is handed the store's conflict, retryable,
/// after one run of business code, with no wait and nothing persisted.
/// The cap is never reached, so C-0726's parking does not apply, and a
/// conflict is not a refusal.
#[test]
fn with_no_race_reruns_a_fact_that_loses_the_race_goes_back_as_the_stores_conflict() {
    let store = store_at_three();
    let orders = Orders::scripted(
        Some(&store),
        vec![Delivery::racing(Reported::Nothing), Delivery::plain()],
    );
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_race_runs: 0,
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(
            &store,
            fact(order(), any(SHIPMENT_DISPATCHED), "shipment-1"),
            &at(1),
            &mut (),
        )
        .ready()
        .err()
        .expect("the race was lost, not parked");
    assert!(
        matches!(
            failure,
            Failure::Kernel(KernelError::Store(crate::StoreError::Conflict { .. }))
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(
        store_conflict(&failure),
        Some(conflict_answer()),
        "keyed (14.51)"
    );
    assert_eq!(orders.seen().len(), 1, "run once");
    assert!(waits.waited().is_empty(), "no wait");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    assert_eq!(stored(&store), notes(1), "nothing of its own persisted");
}

/// A bell that a policy turns into AddItems.
struct Bell;

const RUNG: &str = "toy.bell.v1.Rung";

impl CommandHandler for Bell {
    fn name(&self) -> &str {
        "Bell"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.bell.v1.Ring".to_string()]
    }

    async fn dispatch(&self, _: &ContextualCommand) -> Result<CommandHandlerResponse, CodedError> {
        Ok(CommandHandlerResponse {
            conflict: None,
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any(&format!("/{RUNG}"))),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: "bell-1".to_string(),
        })
    }
}

fn bell_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Bell".to_string(),
        rules_version: "bell-1".to_string(),
        ..order_descriptor()
    }
}

/// Decision 14.46: a policy's step (a reaction, stamped with the head) that
/// loses the race at every write stops at the race re-run cap: it is
/// dead-lettered, and the policy's position follows `on_policy_bug`
/// (past the trigger, by default), as at any cap.
#[test]
fn a_policy_step_losing_every_race_is_dead_lettered_at_the_cap() {
    let store = store_at_three();
    let orders = Orders::scripted(Some(&store), vec![Delivery::racing(Reported::Nothing)]);
    let mut registry = registry_of(orders);
    registry
        .command_handler("bell", Bell, bell_descriptor())
        .unwrap();
    registry.policy_dispatch(PolicyDispatch::new("bell-order", "bell", "order").on_event(
        RUNG,
        |_, _| {
            Ok(vec![CommandBook {
                cover: Some(order()),
                page: Some(CommandPage {
                    body: Some(command_page::Body::Command(Command {
                        payload: Some(any("/toy.order.v1.AddItem")),
                    })),
                    ..CommandPage::default()
                }),
                handling: None,
            }])
        },
    ));
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_race_runs: 3,
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(
            &store,
            command(cover("bell", 1), any("/toy.bell.v1.Ring")),
            &at(1),
            &mut (),
        )
        .ready()
        .err()
        .expect("the step was given up");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::Kernel(KernelError::RetryCapExhausted {
                    attempts: 3,
                    cap: 3,
                    ..
                }),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(orders.seen().len(), 3);
    assert_dead_lettered_at_cap(&log, 3, 3);
    assert_eq!(stored(&store), notes(3), "the step persisted nothing");
    assert_eq!(
        position(&store, "bell-order", &cover("bell", 1)),
        1,
        "past the trigger"
    );
}

/// A race re-run cap of 0 turns race re-runs off for a policy's step too:
/// a step that loses the race at its write comes back like any failure
/// the kernel does not retry: the store's conflict, retryable, after one
/// run of business code, with no wait, nothing dead-lettered and nothing
/// persisted, and the policy's position held at the trigger (never
/// `on_policy_bug`'s move past it), so a resume runs the step again.
#[test]
fn with_no_race_reruns_a_policy_step_that_loses_the_race_goes_back_as_the_stores_conflict() {
    let store = store_at_three();
    let orders = Orders::scripted(
        Some(&store),
        vec![Delivery::racing(Reported::Nothing), Delivery::plain()],
    );
    let mut registry = registry_of(orders);
    registry
        .command_handler("bell", Bell, bell_descriptor())
        .unwrap();
    registry.policy_dispatch(PolicyDispatch::new("bell-order", "bell", "order").on_event(
        RUNG,
        |_, _| {
            Ok(vec![CommandBook {
                cover: Some(order()),
                page: Some(CommandPage {
                    body: Some(command_page::Body::Command(Command {
                        payload: Some(any("/toy.order.v1.AddItem")),
                    })),
                    ..CommandPage::default()
                }),
                handling: None,
            }])
        },
    ));
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_race_runs: 0,
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(
            &store,
            command(cover("bell", 1), any("/toy.bell.v1.Ring")),
            &at(1),
            &mut (),
        )
        .ready()
        .err()
        .expect("the step lost the race");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::Kernel(KernelError::Store(crate::StoreError::Conflict { .. })),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(
        store_conflict(&failure),
        Some(conflict_answer()),
        "keyed (14.51)"
    );
    assert_eq!(orders.seen().len(), 1, "run once");
    assert!(waits.waited().is_empty(), "no wait");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    assert_eq!(stored(&store), notes(1), "the step persisted nothing");
    assert_eq!(
        position(&store, "bell-order", &cover("bell", 1)),
        0,
        "held at the trigger"
    );
}

/// A wire answer: its key, its args in name order, its status.
type Answer = (String, Vec<(String, String)>, GrpcCode);

/// The wire answer of a race lost at a race re-run cap of 0, at the origin
/// or a step down the chain.
fn store_conflict(failure: &Failure<Rejected>) -> Option<Answer> {
    let error = match failure {
        Failure::Kernel(error) => error,
        Failure::Downstream {
            error: StepError::Kernel(error),
            ..
        } => error,
        _ => return None,
    };
    error.as_coded().map(|c| {
        let mut extras: Vec<_> = c.extras.into_iter().collect();
        extras.sort();
        (c.code, extras, c.grpc)
    })
}

/// C-0740 to C-0742 (decision 14.51): UNAVAILABLE
/// `COORDINATOR_REASON_STORE_CONFLICT`, the head stamped (3) and the head
/// the store found (4).
fn conflict_answer() -> Answer {
    (
        CoordinatorReason::StoreConflict.as_str_name().to_string(),
        vec![
            ("actual".to_string(), "4".to_string()),
            ("expected".to_string(), "3".to_string()),
        ],
        GrpcCode::Unavailable,
    )
}

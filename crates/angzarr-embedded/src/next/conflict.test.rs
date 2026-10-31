//! Conflict resolution: all three strategies, COMMUTATIVE the default where
//! the state declares conflict units, STRICT otherwise.
use crate::next::toys::prelude::*;
use crate::next::v1::ConflictResolution;
use crate::next::{Failure, Handled, KernelError, Key, Payload, Recovery, UnitPath, Units};

use crate::next::toys::{
    at, command, cover, handle, opened, payloads, registry, set, shelf, Account, AccountCmd, Edit,
    Shelf, ShelfCmd, ShelfEvent,
};

/// A shelf (stream 3) with items a = 1 then b = 2: sequences 0 and 1.
fn shelf_a_b(store: &MemoryStore) {
    let registry = registry();
    handle::<Shelf>(
        store,
        &registry,
        command(cover("shelf", 3), set("a", 1)).id(1),
        &at(1),
    )
    .unwrap();
    handle::<Shelf>(
        store,
        &registry,
        command(cover("shelf", 3), set("b", 2)).id(2),
        &at(2),
    )
    .unwrap();
}

fn shelf_events(handled: &Handled) -> Vec<ShelfEvent> {
    payloads(&handled.books())
}

/// The shelf's units the window and the command changed, if `failure` is
/// COMMUTATIVE's overlap at `expected` with the head at `actual`.
fn overlap(
    failure: &Failure<crate::next::Rejected>,
    expected: u64,
    actual: u64,
) -> Option<(String, String)> {
    match failure {
        Failure::Kernel(KernelError::Overlap {
            stream,
            expected: e,
            actual: a,
            window_units,
            command_units,
        }) if *stream == cover("shelf", 3) && (*e, *a) == (expected, actual) => {
            Some((window_units.clone(), command_units.clone()))
        }
        _ => None,
    }
}

fn stale(expected: u64, actual: u64) -> Failure<crate::next::Rejected> {
    Failure::Kernel(KernelError::Stale {
        stream: cover("shelf", 3),
        expected,
        actual,
    })
}

#[test]
fn commutative_is_the_default_for_a_state_with_conflict_units() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);

    // Issued when only a existed (expected 1), no resolution asked for: the
    // window changed items[b], the command changes items[a]. Disjoint.
    let disjoint = command(cover("shelf", 3), set("a", 5))
        .id(3)
        .expecting(1, ConflictResolution::Unspecified);
    let handled = handle::<Shelf>(&store, &registry, disjoint, &at(3)).unwrap();
    assert_eq!(
        shelf_events(&handled),
        [ShelfEvent::WasSet(shelf::WasSet {
            item: "a".to_string(),
            count: 5,
            window: None
        })],
        "kept; the kernel resolved, so decide was never handed the window"
    );

    // Issued at the same point, changing items[b], which the window changed.
    let overlapping = command(cover("shelf", 3), set("b", 7))
        .id(4)
        .expecting(1, ConflictResolution::Unspecified);
    let failure = handle::<Shelf>(&store, &registry, overlapping, &at(4))
        .err()
        .unwrap();
    assert_eq!(
        overlap(&failure, 1, 3),
        Some(("items[a],items[b]".to_string(), "items[b]".to_string())),
        "the overlap names both unit sets, the window from 1 to 3: {failure:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
}

#[test]
fn strict_is_the_default_for_a_state_without_conflict_units() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 100);

    let behind = command(cover("account", 1), AccountCmd::Deposit(1).to_any())
        .id(3)
        .expecting(1, ConflictResolution::Unspecified);
    assert_eq!(
        handle::<Account>(&store, &registry, behind, &at(20))
            .err()
            .unwrap(),
        Failure::Kernel(KernelError::Stale {
            stream: cover("account", 1),
            expected: 1,
            actual: 2
        }),
        "refused before decide"
    );
}

#[test]
fn a_command_type_configured_strict_is_strict() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);
    let note = command(
        cover("shelf", 3),
        ShelfCmd::Note("restock".to_string()).to_any(),
    )
    .id(3)
    .expecting(1, ConflictResolution::Unspecified);
    assert_eq!(
        handle::<Shelf>(&store, &registry, note, &at(3))
            .err()
            .unwrap(),
        stale(1, 2),
        "a note is never merged, though the shelf is COMMUTATIVE"
    );
}

#[test]
fn an_unmarked_field_conflicts_with_everything() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);
    let note = command(
        cover("shelf", 3),
        ShelfCmd::Note("restock".to_string()).to_any(),
    )
    .id(3);
    handle::<Shelf>(&store, &registry, note, &at(3)).unwrap();

    // Behind the note: the window changed `notes`, which carries no marker.
    let behind = command(cover("shelf", 3), set("c", 1))
        .id(4)
        .expecting(2, ConflictResolution::Commutative);
    let failure = handle::<Shelf>(&store, &registry, behind, &at(4))
        .err()
        .unwrap();
    assert!(
        overlap(&failure, 2, 3).is_some_and(|(window, _)| window.starts_with('*')),
        "never a blind merge: {failure:?}"
    );
}

#[test]
fn aggregate_handles_demands_the_nonce_back() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);

    let handled = handle::<Shelf>(
        &store,
        &registry,
        command(cover("shelf", 3), set("b", 7))
            .id(3)
            .expecting(1, ConflictResolution::AggregateHandles),
        &at(3),
    )
    .unwrap();
    assert_eq!(
        shelf_events(&handled),
        [ShelfEvent::WasSet(shelf::WasSet {
            item: "b".to_string(),
            count: 7,
            window: Some(1)
        })],
        "decide saw the one event it missed, and acknowledged it"
    );

    let unresolved = shelf::Set {
        item: "a".to_string(),
        count: 9,
        resolve: false,
    };
    let failure = handle::<Shelf>(
        &store,
        &registry,
        command(cover("shelf", 3), crate::next::pack(&unresolved))
            .id(4)
            .expecting(1, ConflictResolution::AggregateHandles),
        &at(4),
    )
    .err()
    .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Unresolved {
            stream: cover("shelf", 3),
            expected: 1,
            actual: 3
        }),
        "a decide that never acknowledged the window fails as stale"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
}

/// Outside STRICT, a command with an expected sequence whose write is lost
/// to another write to its stream is never answered the stale expected
/// sequence: that key is STRICT's alone (decision 14.45, amended). Under
/// AGGREGATE_HANDLES its window is re-delivered under the retry cap; with
/// retrying off that cap is its one delivery, so it is given up at once,
/// `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED` (decision 14.46).
#[test]
fn outside_strict_a_lost_write_is_never_the_stale_key() {
    let registry = registry();
    let store = crate::next::recording::Recording::new();
    for (n, item) in [(1u128, "a"), (2, "b")] {
        handle::<Shelf>(
            &store,
            &registry,
            command(cover("shelf", 3), set(item, n as i64)).id(n),
            &at(n as i64),
        )
        .unwrap();
    }
    store.fail_write_with(
        0,
        crate::next::StoreError::Conflict {
            stream: Box::new(cover("shelf", 3)),
            expected: 2,
            actual: 3,
        },
    );
    let failure = handle::<Shelf>(
        &store,
        &registry,
        command(cover("shelf", 3), set("b", 7))
            .id(3)
            .expecting(1, ConflictResolution::AggregateHandles),
        &at(3),
    )
    .err()
    .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::RetryCapExhausted {
            stream: cover("shelf", 3),
            attempts: 1,
            cap: 1,
        })
    );
}

/// An expected sequence past the head is malformed under every
/// resolution (decision 14.47), never stale.
#[test]
fn an_expected_sequence_past_the_head_is_ahead() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);
    for (n, resolution) in [
        ConflictResolution::Strict,
        ConflictResolution::AggregateHandles,
        ConflictResolution::Commutative,
    ]
    .into_iter()
    .enumerate()
    {
        let ahead = command(cover("shelf", 3), set("z", 1))
            .id(10 + n as u128)
            .expecting(5, resolution);
        assert_eq!(
            handle::<Shelf>(&store, &registry, ahead, &at(5))
                .err()
                .unwrap(),
            Failure::Kernel(KernelError::ExpectedSequenceAhead {
                stream: cover("shelf", 3),
                expected: 5,
                head: 2,
            }),
            "{resolution:?}"
        );
    }
}

#[test]
fn unit_paths_follow_angzarrs_syntax() {
    let items = UnitPath::field("items");
    let path = |p: &UnitPath| p.to_string();
    assert_eq!(
        path(&items.key(Key::Uint(42)).dot("weight")),
        "items[42].weight"
    );
    assert_eq!(path(&items.key(Key::Int(-1))), "items[-1]");
    assert_eq!(path(&items.key(Key::Bool(true))), "items[true]");
    assert_eq!(path(&UnitPath::field("columns").order()), "columns[#order]");
    assert_eq!(
        path(&items.key(Key::Str("#order"))),
        "items[\\#order]",
        "a key never starts with a bare #, so it never collides with [#order]"
    );
    assert_eq!(path(&items.key(Key::Str("a]b\\c"))), "items[a\\]b\\\\c]");
    assert_eq!(
        items.key(Key::Bytes(&[0xff, b']'])).as_bytes(),
        b"items[\xff\\]]",
        "bytes keys are raw"
    );
    assert_eq!(path(&UnitPath::field("notes").wildcard()), "*notes");
    assert!(UnitPath::unknown().is_wildcard());
}

#[test]
fn a_state_without_conflict_units_changes_everything() {
    use crate::next::State;
    let state = crate::next::toys::account::AccountState::default();
    assert_eq!(
        <crate::next::toys::account::AccountState as State>::changed(&state, &state),
        Units::everything(),
        "the fail-safe default: never a blind merge"
    );
}

#[test]
fn a_fact_keeps_its_sent_correlation() {
    let registry = registry();
    let store = MemoryStore::new();
    let payment = crate::next::toys::fact(
        cover("account", 1),
        crate::next::toys::AccountFact::PaymentReceived(5).to_any(),
        "pi_c",
    )
    .correlated(4);
    let handled = handle::<Account>(&store, &registry, payment, &at(1)).unwrap();
    assert_eq!(
        handled.books()[0].pages[0]
            .meta
            .as_ref()
            .unwrap()
            .correlation_id,
        Some(crate::next::toys::correlation(4))
    );
}

/// The old note command: upcast to `toy.shelf.cmd.v1.Note`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OldNote {
    #[prost(string, tag = "1")]
    pub text: String,
}

impl prost::Name for OldNote {
    const NAME: &'static str = "Note";
    const PACKAGE: &'static str = "toy.shelf.cmd.v0";
}

#[test]
fn an_old_command_type_has_its_current_types_resolution() {
    use crate::next::{Registry, Upcaster};
    let mut registry = Registry::new();
    registry
        .register::<Shelf>(
            Upcaster::new("shelf-upcaster", "shelf")
                .rule::<OldNote, shelf::Note>(|old| Ok(shelf::Note { note: old.text })),
            crate::next::toys::handlers::<crate::next::toys::Shelf>(),
        )
        .unwrap();
    let store = MemoryStore::new();
    for (n, item) in [(1u128, "a"), (2, "b")] {
        handle::<Shelf>(
            &store,
            &registry,
            command(cover("shelf", 3), set(item, 1)).id(n),
            &at(n as i64),
        )
        .unwrap();
    }
    let old = || {
        crate::next::pack(&OldNote {
            text: "restock".to_string(),
        })
    };
    assert_eq!(
        handle::<Shelf>(
            &store,
            &registry,
            command(cover("shelf", 3), old())
                .id(3)
                .expecting(1, ConflictResolution::Unspecified),
            &at(3),
        )
        .err()
        .unwrap(),
        stale(1, 2),
        "configured STRICT by its current type, though sent as v0"
    );
    let handled = handle::<Shelf>(
        &store,
        &registry,
        command(cover("shelf", 3), old()).id(4),
        &at(4),
    )
    .unwrap();
    assert_eq!(
        shelf_events(&handled),
        [ShelfEvent::Noted("restock".to_string())]
    );

    // An upcast that fails is looked up as sent, and decoding reports it.
    let mut failing = Registry::new();
    failing
        .register::<Shelf>(
            Upcaster::new("shelf-upcaster", "shelf")
                .rule::<OldNote, shelf::Note>(|_| Err(angzarr_diagnostic::Report::new("toy-no"))),
            crate::next::toys::handlers::<crate::next::toys::Shelf>(),
        )
        .unwrap();
    assert!(matches!(
        handle::<Shelf>(
            &store,
            &failing,
            command(cover("shelf", 3), old()).id(5),
            &at(5)
        ),
        Err(Failure::Kernel(KernelError::Upcast { .. }))
    ));
}

/// A state that lists COMMUTATIVE yet keeps [`State::changed`]'s default, so
/// every change it reports is "everything": only a command with no window
/// between its expected sequence and the head commits.
mod tally {
    use std::convert::Infallible;

    use crate::next::angzarr_router::domain::DomainDispatch;
    use crate::next::v1::{ConflictResolution, StateDescriptor, StateOptions};
    use crate::next::{pack, Decoder, Domain, Payload, PayloadError, State};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Tally {
        #[prost(uint32, tag = "1")]
        pub count: u32,
    }

    impl prost::Name for Tally {
        const NAME: &'static str = "Tally";
        const PACKAGE: &'static str = "toy.tally.v1";
    }

    impl State for Tally {
        type Snapshot = Self;

        fn to_snapshot(&self) -> Self {
            self.clone()
        }

        fn from_snapshot(snapshot: Self) -> Result<Self, angzarr_diagnostic::Report> {
            Ok(snapshot)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/toy.tally.v1.Tally".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Commutative as i32,
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }
        }
    }

    /// The command and the event it becomes.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Count {}

    impl prost::Name for Count {
        const NAME: &'static str = "Count";
        const PACKAGE: &'static str = "toy.tally.v1";
    }

    impl Payload for Count {
        fn to_any(&self) -> crate::next::Any {
            pack(self)
        }

        fn from_any(any: &crate::next::Any) -> Result<Self, PayloadError> {
            Decoder::new().on::<Count>(|c| c).decode(any)
        }

        fn type_urls() -> Vec<String> {
            Vec::new()
        }
    }

    pub struct Counter;

    impl Domain for Counter {
        type State = Tally;
        type Command = Count;
        type Fact = Infallible;
        type Event = Count;
        type Rejection = Infallible;
        const RULES_VERSION: &'static str = "v1";

        const NAME: &'static str = "tally";

        fn apply(state: &mut Tally, _: &Count) {
            state.count += 1;
        }
    }

    /// The counter's one handler, on the router's table: every count
    /// counts.
    pub fn handlers(table: DomainDispatch<Tally>) -> DomainDispatch<Tally> {
        table.on_command("toy.tally.v1.Count", |_, _, _| {
            Ok(Some(crate::next::v1::EventBook {
                pages: vec![crate::next::v1::EventPage {
                    payload: Some(pack(&Count {})),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        })
    }
}

#[test]
fn a_commutative_command_at_the_head_has_no_window_whatever_its_units() {
    use crate::next::{Registry, Upcaster};
    let mut registry = Registry::new();
    registry
        .register::<tally::Counter>(Upcaster::new("tally-upcaster", "tally"), tally::handlers)
        .unwrap();
    let store = MemoryStore::new();
    let count = |n: u128| command(cover("tally", 1), crate::next::pack(&tally::Count {})).id(n);
    for n in 1..=2 {
        assert!(
            handle::<tally::Counter>(&store, &registry, count(n), &at(n as i64)).is_ok(),
            "stamped at the head: nothing between, nothing to overlap"
        );
    }
    assert!(handle::<tally::Counter>(
        &store,
        &registry,
        count(3).expecting(2, ConflictResolution::Commutative),
        &at(3),
    )
    .is_ok());
    assert_eq!(
        handle::<tally::Counter>(
            &store,
            &registry,
            count(4).expecting(1, ConflictResolution::Commutative),
            &at(4),
        )
        .err(),
        Some(Failure::Kernel(KernelError::Overlap {
            stream: cover("tally", 1),
            expected: 1,
            actual: 3,
            window_units: "*".to_string(),
            command_units: "*".to_string(),
        })),
        "a window, and every change overlaps it"
    );
}

/// The kernel judges the router's conflict report (decision 14.42): the
/// shelf's router table behind a handler that rewrites the report it
/// answers with `edit`, everything else as the router answered.
mod judged {
    use std::sync::Arc;

    use crate::next::angzarr_router::error::CodedError;
    use crate::next::angzarr_router::pb;
    use crate::next::angzarr_router::router::CommandHandler;
    use crate::next::toys::prelude::*;
    use crate::next::toys::{at, command, cover, handlers, set, Edit, Shelf, ShelfState};
    use crate::next::v1::ConflictResolution;
    use crate::next::{Failure, Handled, KernelError, Registry};

    use super::shelf_a_b;

    struct Rewritten {
        inner: Box<dyn crate::next::angzarr_router::router::DynCommandHandler>,
        edit: fn(&mut pb::ConflictReport),
    }

    impl CommandHandler for Rewritten {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn command_types(&self) -> Vec<String> {
            self.inner.command_types()
        }
        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<pb::CommandHandlerResponse, CodedError> {
            let mut response = self.inner.dispatch(req).await?;
            (self.edit)(response.conflict.get_or_insert_with(Default::default));
            Ok(response)
        }
    }

    fn rewritten(edit: fn(&mut pb::ConflictReport)) -> Registry {
        let table = crate::next::route::table::<Shelf>(Arc::new(ShelfState::default), false);
        let inner = handlers::<Shelf>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "shelf",
                Rewritten {
                    inner: Box::new(inner),
                    edit,
                },
                crate::next::toys::descriptor_of::<Shelf>(
                    crate::next::v1::ComponentKind::Aggregate,
                    "v1",
                    &[],
                ),
            )
            .unwrap();
        registry
    }

    fn handled(
        store: &MemoryStore,
        registry: &Registry,
        book: crate::next::v1::CommandBook,
    ) -> Result<Handled, Failure<crate::next::Rejected>> {
        let log = crate::next::MemoryDeadLetters::new();
        crate::next::Kernel::new(registry, &log)
            .on_retryable(crate::next::toys::no_retries())
            .handle_any(store, book, &at(3), &mut ())
            .ready()
    }

    /// C-0683: under AGGREGATE_HANDLES the kernel accepts the events only if
    /// the nonce reported is the one it delivered.
    #[test]
    fn an_unresolved_nonce_reported_is_aborted_by_the_kernel() {
        let store = MemoryStore::new();
        shelf_a_b(&store);
        let book = || {
            command(cover("shelf", 3), set("b", 7))
                .id(3)
                .expecting(1, ConflictResolution::AggregateHandles)
        };
        let forged = rewritten(|report| report.resolved_nonce = b"forged".to_vec());
        let failure = handled(&store, &forged, book()).err().unwrap();
        assert_eq!(
            failure,
            Failure::Kernel(KernelError::Unresolved {
                stream: cover("shelf", 3),
                expected: 1,
                actual: 2
            })
        );
        let never = rewritten(|report| report.resolved_nonce = Default::default());
        assert!(matches!(
            handled(&store, &never, book()),
            Err(Failure::Kernel(KernelError::Unresolved { .. }))
        ));
        assert_eq!(
            pages(&store, &cover("shelf", 3)).len(),
            2,
            "nothing persisted"
        );

        let honest = rewritten(|_| {});
        handled(&store, &honest, book()).unwrap();
        assert_eq!(pages(&store, &cover("shelf", 3)).len(), 3);
    }

    /// C-0684: under COMMUTATIVE the kernel keeps the events only if the
    /// window's units and the command's do not overlap (options.proto,
    /// Overlap): equal, a prefix at a `.` or `[` boundary, or a wildcard
    /// against a non-empty set.
    #[test]
    fn overlapping_units_reported_are_aborted_by_the_kernel() {
        let book = |n| {
            command(cover("shelf", 3), set("a", 5))
                .id(n)
                .expecting(1, ConflictResolution::Commutative)
        };
        let stale = |store: &MemoryStore, edit| {
            let failure = handled(store, &rewritten(edit), book(3)).err();
            assert_eq!(
                pages(store, &cover("shelf", 3)).len(),
                2,
                "nothing persisted"
            );
            failure
        };
        let aborted = |edit: fn(&mut pb::ConflictReport)| {
            let mut report = pb::ConflictReport::default();
            edit(&mut report);
            Some(Failure::Kernel(KernelError::Overlap {
                stream: cover("shelf", 3),
                expected: 1,
                actual: 2,
                window_units: report.window_units.join(","),
                command_units: report.command_units.join(","),
            }))
        };
        for edit in [
            (|r: &mut pb::ConflictReport| {
                r.window_units = vec!["items[a]".to_string()];
                r.command_units = vec!["items[a]".to_string()];
            }) as fn(&mut pb::ConflictReport),
            |r| {
                r.window_units = vec!["items[a]".to_string()];
                r.command_units = vec!["items[a].count".to_string()];
            },
            |r| {
                r.window_units = vec!["items[a].count".to_string()];
                r.command_units = vec!["items".to_string()];
            },
            |r| {
                r.window_units = vec!["*notes".to_string()];
                r.command_units = vec!["title".to_string()];
            },
        ] {
            let store = MemoryStore::new();
            shelf_a_b(&store);
            assert_eq!(stale(&store, edit), aborted(edit));
        }

        for edit in [
            (|r: &mut pb::ConflictReport| {
                r.window_units = vec!["items[a]".to_string()];
                r.command_units = vec!["items[ab]".to_string()];
            }) as fn(&mut pb::ConflictReport),
            |r| {
                r.window_units = vec!["*".to_string()];
                r.command_units = Vec::new();
            },
        ] {
            let store = MemoryStore::new();
            shelf_a_b(&store);
            handled(&store, &rewritten(edit), book(3)).unwrap();
            assert_eq!(pages(&store, &cover("shelf", 3)).len(), 3, "disjoint: kept");
        }
    }
}

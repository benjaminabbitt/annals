#[allow(unused_imports)]
use crate::next::toys::prelude::*;
mod dedup {
    //! Step 3: deduplication by `command_id` on the events, with its accepted
    //! costs. No claims, no fingerprints, no commands table.
    use crate::next::toys::prelude::*;
    use crate::next::v1::DeliveryOutcome;
    use crate::next::{Failure, Handled, Payload};

    use crate::next::toys::{
        at, command, cover, fact, handle, opened, payloads, registry, AccountCmd, AccountEvent,
        AccountFact, Edit,
    };

    fn deposit(n: u128, cents: i64) -> crate::next::v1::CommandBook {
        command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
    }

    #[test]
    fn a_retried_command_is_a_duplicate_answered_with_its_events() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        let first =
            handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20))
                .unwrap();
        assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
        let retry =
            handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(21))
                .unwrap();
        assert_eq!(retry.outcome().delivery, DeliveryOutcome::Duplicate as i32);
        assert!(matches!(retry, Handled::Duplicate(_)));
        assert_eq!(
            retry.books(),
            first.books(),
            "the first delivery's events, re-read"
        );
        assert_eq!(
            payloads::<AccountEvent>(&retry.books()),
            [AccountEvent::Deposited(25)]
        );
        let page = &retry.books()[0].pages[0];
        assert_eq!(
            page.meta.as_ref().unwrap().recorded_at.unwrap().seconds,
            20,
            "nothing re-stamped"
        );
    }

    #[test]
    fn a_zero_event_decision_is_decided_again() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);

        // A sweep of an empty account withdraws nothing: applied, no events.
        let sweep = || command(cover("account", 1), AccountCmd::Sweep.to_any()).id(7);
        let first =
            handle::<crate::next::toys::Account>(&store, &registry, sweep(), &at(20)).unwrap();
        assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
        assert!(payloads::<AccountEvent>(&first.books()).is_empty());

        // Money arrives; the same sweep is redelivered.
        let payment = fact(
            cover("account", 1),
            AccountFact::PaymentReceived(50).to_any(),
            "pi_1",
        );
        handle::<crate::next::toys::Account>(&store, &registry, payment, &at(21)).unwrap();
        let again =
            handle::<crate::next::toys::Account>(&store, &registry, sweep(), &at(22)).unwrap();
        assert_eq!(
            again.outcome().delivery,
            DeliveryOutcome::Applied as i32,
            "the accepted cost: re-decided against the stream as it is now"
        );
        assert_eq!(
            payloads::<AccountEvent>(&again.books()),
            [AccountEvent::Withdrew(49)],
            "50 less the v2 fee"
        );
    }

    #[test]
    fn a_creating_command_redelivered_after_a_purge_creates_its_stream_again() {
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);
        let open = || command(stream.clone(), AccountCmd::Open.to_any()).id(5);
        handle::<crate::next::toys::Account>(&store, &registry, open(), &at(20)).unwrap();
        let retry =
            handle::<crate::next::toys::Account>(&store, &registry, open(), &at(21)).unwrap();
        assert_eq!(retry.outcome().delivery, DeliveryOutcome::Duplicate as i32);

        assert_eq!(store.purge(sref(&stream), 1).ready(), Ok(1));
        let again =
            handle::<crate::next::toys::Account>(&store, &registry, open(), &at(22)).unwrap();
        assert_eq!(
            again.outcome().delivery,
            DeliveryOutcome::Applied as i32,
            "the accepted cost: nothing left to find, so decided again"
        );
        assert_eq!(
            payloads::<AccountEvent>(&again.books()),
            [AccountEvent::Opened]
        );
        let slice = slice(&store, &stream);
        assert_eq!(slice.next_sequence, 1, "a new stream, from sequence 0");
        assert_eq!(slice.last_recorded_at.unwrap().seconds, 22);
    }

    #[test]
    fn a_reused_command_id_with_another_payload_is_answered_with_the_first() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        let reused =
            handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 999), &at(21))
                .unwrap();
        assert_eq!(
            reused.outcome().delivery,
            DeliveryOutcome::Duplicate as i32,
            "the accepted cost: payloads are not fingerprinted"
        );
        assert_eq!(
            payloads::<AccountEvent>(&reused.books()),
            [AccountEvent::Deposited(25)]
        );
    }

    #[test]
    fn the_same_command_id_to_another_stream_is_another_command() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        opened(&store, &registry, 2, 100);

        handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        let other = command(cover("account", 2), AccountCmd::Deposit(25).to_any()).id(3);
        let applied =
            handle::<crate::next::toys::Account>(&store, &registry, other, &at(21)).unwrap();
        assert_eq!(applied.outcome().delivery, DeliveryOutcome::Applied as i32);
    }

    #[test]
    fn a_redelivered_fact_is_a_duplicate() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let payment = || {
            fact(
                cover("account", 1),
                AccountFact::PaymentReceived(50).to_any(),
                "pi_1234",
            )
        };

        handle::<crate::next::toys::Account>(&store, &registry, payment(), &at(20)).unwrap();
        let again =
            handle::<crate::next::toys::Account>(&store, &registry, payment(), &at(21)).unwrap();
        assert_eq!(again.outcome().delivery, DeliveryOutcome::Duplicate as i32);
        let meta = again.books()[0].pages[0].meta.clone().unwrap();
        assert_eq!(
            meta.causation,
            Some(crate::next::v1::event_meta::Causation::CommandId(
                crate::next::ids::fact_command_id("pi_1234")
            )),
            "the id is on the event, where the lookup finds it"
        );
    }

    #[test]
    fn a_rejection_leaves_nothing_and_a_retry_runs_again() {
        let registry = registry();
        let store = MemoryStore::new();
        let early = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);

        let refused = handle::<crate::next::toys::Account>(&store, &registry, early(), &at(5));
        assert!(matches!(
            refused,
            Err(Failure::Rejected(r)) if angzarr_diagnostic::Diagnostic::code(r.report()) == "toy-account-not-open"
        ));
        assert!(streams(&store).is_empty());

        opened(&store, &registry, 1, 0);
        let retried =
            handle::<crate::next::toys::Account>(&store, &registry, early(), &at(20)).unwrap();
        assert_eq!(retried.outcome().delivery, DeliveryOutcome::Applied as i32);
    }

    /// A store that plays READ COMMITTED with a concurrent commit: it
    /// already holds the original delivery's events, but the first read
    /// made of it (whichever it is) sees the stream as it was before them;
    /// every later read, and every write, sees the store as it is.
    struct CommitLandsAfterFirstRead {
        store: MemoryStore,
        /// The stream's length before the original commit.
        before: u64,
        seen: std::sync::atomic::AtomicBool,
    }

    impl CommitLandsAfterFirstRead {
        fn first_read(&self) -> bool {
            !self.seen.swap(true, std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::next::EventStore for CommitLandsAfterFirstRead {
        async fn write(
            &self,
            write: crate::next::Write<'_>,
        ) -> Result<crate::next::Written, crate::next::StoreError> {
            self.store.write(write).await
        }

        async fn read(
            &self,
            stream: crate::next::StreamRef<'_>,
            from: u64,
            to: Option<u64>,
        ) -> Result<crate::next::Slice, crate::next::StoreError> {
            let early = self.first_read();
            let mut slice = self.store.read(stream, from, to).await?;
            if early {
                slice.pages.retain(|p| p.sequence < self.before);
                slice.next_sequence = self.before;
            }
            Ok(slice)
        }

        async fn find_command(
            &self,
            stream: crate::next::StreamRef<'_>,
            command_id: angzarr_ports::Uuid,
        ) -> Result<Vec<crate::next::v1::EventPage>, crate::next::StoreError> {
            let early = self.first_read();
            let mut pages = self.store.find_command(stream, command_id).await?;
            if early {
                pages.retain(|p| p.sequence < self.before);
            }
            Ok(pages)
        }

        async fn covers(
            &self,
            domain: &str,
        ) -> Result<Vec<crate::next::v1::Cover>, crate::next::StoreError> {
            self.store.covers(domain).await
        }
    }

    impl crate::next::SnapshotStore for CommitLandsAfterFirstRead {
        async fn read_snapshot(
            &self,
            stream: crate::next::StreamRef<'_>,
        ) -> Result<Option<crate::next::v1::Snapshot>, crate::next::StoreError> {
            self.store.read_snapshot(stream).await
        }

        async fn write_snapshot(
            &self,
            stream: crate::next::StreamRef<'_>,
            snapshot: &crate::next::v1::Snapshot,
        ) -> Result<(), crate::next::StoreError> {
            self.store.write_snapshot(stream, snapshot).await
        }
    }

    impl crate::next::PositionStore for CommitLandsAfterFirstRead {
        async fn position(
            &self,
            key: crate::next::PositionKey<'_>,
        ) -> Result<u64, crate::next::StoreError> {
            self.store.position(key).await
        }
    }

    #[test]
    fn a_retry_racing_its_original_commit_is_never_applied_twice() {
        // The stream is loaded before the command is looked up, so an original
        // that commits between the retry's two reads is seen by the lookup.
        // (Looked up first, the retry would find nothing, then load the head
        // after the original's events and append again.)
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let before = pages(&store, &cover("account", 1)).len() as u64;
        handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();

        let log = crate::next::MemoryDeadLetters::new();
        let kernel = crate::next::Kernel::new(&registry, &log);
        let racing = CommitLandsAfterFirstRead {
            store: store.clone(),
            before,
            seen: std::sync::atomic::AtomicBool::new(false),
        };
        let retry = kernel
            .handle::<crate::next::toys::Account, _, _>(&racing, deposit(3, 25), &at(21), &mut ())
            .ready()
            .unwrap();
        assert!(
            matches!(retry, Handled::Duplicate(_)),
            "found, not re-applied"
        );
        let deposits = pages(&store, &cover("account", 1)).len() as u64;
        assert_eq!(deposits, before + 1, "the deposit is on the stream once");
    }
}

mod pipeline {
    //! The normative order: validate → identify → load, then deduplicate →
    //! resolve → decode (and upcast) → decide or ingest. Each test pins one
    //! boundary by sending something that two adjacent steps would answer
    //! differently.
    use crate::next::toys::prelude::*;

    use super::super::{steps::Coordinator, Deduplicated};
    use crate::next::v1::{ConflictResolution, DeliveryOutcome, EventRef};
    use crate::next::{check, pack, Failure, KernelError, Malformed, Payload, Recovery};

    use crate::next::toys::{
        account, at, command, cover, handle, opened, registry, Account, AccountCmd, Edit,
    };

    fn deposit(n: u128) -> crate::next::v1::CommandBook {
        command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(n)
    }

    #[test]
    fn an_applied_strict_retry_is_a_duplicate_never_stale() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        let at_head = || deposit(3).expecting(2, ConflictResolution::Strict);
        handle::<Account>(&store, &registry, at_head(), &at(20)).unwrap();
        handle::<Account>(&store, &registry, deposit(4), &at(21)).unwrap();

        // The stream is at 4 now; the retry still expects 2.
        let retry = handle::<Account>(&store, &registry, at_head(), &at(22)).unwrap();
        assert_eq!(
            retry.outcome().delivery,
            DeliveryOutcome::Duplicate as i32,
            "the retry's events are found before its sequence is checked"
        );

        // A new command expecting 2 is stale, as STRICT says.
        let stale = handle::<Account>(
            &store,
            &registry,
            deposit(5).expecting(2, ConflictResolution::Strict),
            &at(23),
        )
        .err()
        .unwrap();
        assert_eq!(
            stale,
            Failure::Kernel(KernelError::Stale {
                stream: cover("account", 1),
                expected: 2,
                actual: 4
            })
        );
        assert_eq!(stale.recovery(), Recovery::Retry);
    }

    #[test]
    fn a_malformed_retry_of_an_applied_command_is_still_malformed() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        handle::<Account>(&store, &registry, deposit(3), &at(20)).unwrap();

        let forged = deposit(3).caused_by(EventRef {
            stream: Some(cover("account", 1)),
            sequence: 0,
        });
        let refused = handle::<Account>(&store, &registry, forged, &at(21))
            .err()
            .unwrap();
        assert_eq!(
            refused,
            Failure::Kernel(KernelError::Malformed(Malformed::CausationFromOutside)),
            "refused before anything is looked up"
        );
        assert_eq!(refused.recovery(), Recovery::Invalid);
    }

    #[test]
    fn an_unknown_command_type_is_refused_at_decode() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let unknown = || command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 }));

        let refused = handle::<Account>(&store, &registry, unknown().id(8), &at(20))
            .err()
            .unwrap();
        assert!(
            matches!(
                &refused,
                Failure::Kernel(KernelError::Router(e))
                    if e.code == "ROUTER_REASON_UNKNOWN_COMMAND_TYPE"
                        && e.extras.get("type_url").map(String::as_str)
                            == Some("/toy.account.v9.Bonus")
            ),
            "the router's ROUTER_REASON_UNKNOWN_COMMAND_TYPE, named by its type URL: {refused:?}"
        );
        // The router's own error on malformed input is a bug, INTERNAL (the
        // owner's ruling, 2026-10-29), never the sender's to fix.
        assert_eq!(refused.recovery(), Recovery::Bug);

        // Stale and undecodable: STRICT's check comes first.
        let stale = handle::<Account>(
            &store,
            &registry,
            unknown().id(9).expecting(1, ConflictResolution::Strict),
            &at(21),
        )
        .err()
        .unwrap();
        assert!(matches!(stale, Failure::Kernel(KernelError::Stale { .. })));

        // Applied under that id already: the duplicate is found first, the
        // payload never decoded (and never compared: a reused id goes
        // undetected).
        let dup = handle::<Account>(
            &store,
            &registry,
            command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 })).id(2),
            &at(22),
        )
        .unwrap();
        assert_eq!(dup.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    }

    #[test]
    fn the_coordinator_runs_dedup_then_load_then_decode() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);

        // An applied command: step 3 answers it.
        let applied = check(deposit(2), &registry).unwrap().identify(&at(30));
        assert!(matches!(
            coordinator.deduplicate(applied).unwrap(),
            Deduplicated::Duplicate { .. }
        ));

        // A new one goes on: stamped at the head, STRICT, no window, no nonce.
        let novel = check(deposit(3), &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(novel).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(novel.head(), 2, "the head as step 3 loaded it");
        let contextual = coordinator.load(novel).unwrap();
        assert_eq!(contextual.conflict_resolution(), ConflictResolution::Strict);
        assert_eq!(contextual.expected_sequence(), 2, "stamped with the head");
        assert_eq!(contextual.head(), 2);
        assert!(contextual.window().is_empty());
        assert!(!contextual.has_nonce());

        let prepared = contextual.decode::<Account>(&registry).unwrap();
        assert_eq!(prepared.stream().state().cents, 100);
    }

    #[test]
    fn the_coordinator_hands_a_windowed_command_its_window_and_a_nonce() {
        use crate::next::toys::{set, Shelf};
        let registry = registry();
        let store = MemoryStore::new();
        for (n, item) in ["a", "b", "c"].into_iter().enumerate() {
            handle::<Shelf>(
                &store,
                &registry,
                command(cover("shelf", 3), set(item, 1)).id(n as u128 + 1),
                &at(n as i64),
            )
            .unwrap();
        }
        let mut coordinator = Coordinator::new(&store, &registry);
        let book = command(cover("shelf", 3), set("d", 1))
            .id(9)
            .expecting(1, ConflictResolution::AggregateHandles);
        let identified = check(book, &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(
            novel.command().command_id(),
            &crate::next::toys::command_id(9)
        );
        assert_eq!(novel.head(), 3);
        let contextual = coordinator.load(novel).unwrap();
        assert_eq!(
            contextual.conflict_resolution(),
            ConflictResolution::AggregateHandles
        );
        assert_eq!(contextual.expected_sequence(), 1);
        assert_eq!(contextual.head(), 3);
        assert_eq!(contextual.pages().len(), 3);
        assert_eq!(
            contextual
                .window()
                .iter()
                .map(|p| p.sequence)
                .collect::<Vec<_>>(),
            [1, 2],
            "from the expected sequence to the head"
        );
        assert!(contextual.has_nonce());
        assert!(contextual.snapshot().is_none());
        assert_eq!(
            contextual.command().command_id(),
            &crate::next::toys::command_id(9)
        );
    }

    #[test]
    fn the_coordinators_decision_commits_to_its_stream() {
        let registry = registry();
        let log = crate::next::MemoryDeadLetters::new();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);
        let identified = check(deposit(3), &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let prepared = coordinator
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        assert_eq!(
            prepared.command().command_id(),
            &crate::next::toys::command_id(3)
        );
        let pages = prepared.decide(&at(30), &registry, &log).unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].sequence, 2, "at the head");
        let mut stream = prepared.into_stream();
        assert_eq!(stream.commit(&pages, &registry, &mut []), Ok(3));
        assert_eq!(stream.state().cents, 105);

        opened(&store, &registry, 2, 0);
        let sweep = check(
            command(cover("account", 2), AccountCmd::Sweep.to_any()).id(5),
            &registry,
        )
        .unwrap()
        .identify(&at(32));
        let mut empty = Coordinator::new(&store, &registry);
        let Deduplicated::Novel(novel) = empty.deduplicate(sweep).unwrap() else {
            panic!("never applied");
        };
        let prepared = empty
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        let pages = prepared.decide(&at(32), &registry, &log).unwrap();
        assert!(pages.is_empty(), "a sweep of nothing withdraws nothing");

        // A cover of another domain than the one decoded for.
        let mut wrong = Coordinator::new(&store, &registry);
        let identified = check(deposit(6), &registry).unwrap().identify(&at(33));
        let Deduplicated::Novel(novel) = wrong.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(
            wrong
                .load(novel)
                .unwrap()
                .decode::<crate::next::toys::Ledger>(&registry)
                .err(),
            Some(KernelError::Replay(crate::next::ReplayError::WrongDomain {
                found: "account".to_string(),
                expected: "ledger"
            }))
        );
    }
}

mod snapshot {
    //! Snapshots: a framework optimisation, never business data. Used only
    //! under the current rules version and state type, and never one that
    //! hides part of a conflict window.
    use crate::next::toys::prelude::*;

    use super::super::{steps::Coordinator, Deduplicated};
    use crate::next::v1::{ConflictResolution, Snapshot};
    use crate::next::{check, pack, Payload, Stream};

    use crate::next::toys::{
        account, at, command, cover, opened, registry, root, set, shelf, Account, AccountCmd, Edit,
        Shelf,
    };

    /// A snapshot of an account at sequence 1 claiming `cents`: a poisoned one
    /// shows when it is used.
    fn poisoned(rules: &str, cents: i64) -> Snapshot {
        Snapshot {
            sequence: 1,
            state: Some(pack(&account::AccountState {
                open: true,
                cents,
                ..Default::default()
            })),
            rules_version: rules.to_string(),
        }
    }

    /// Load (steps 2 to 5) a deposit for account 1, expecting `expected` under
    /// `resolution` if given.
    fn load(
        store: &MemoryStore,
        expected: Option<(u64, ConflictResolution)>,
    ) -> (Option<u64>, i64) {
        let registry = registry();
        let mut book = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(9);
        if let Some((sequence, resolution)) = expected {
            book = book.expecting(sequence, resolution);
        }
        let mut coordinator = Coordinator::new(store, &registry);
        let identified = check(book, &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let contextual = coordinator.load(novel).unwrap();
        let used = contextual.snapshot().map(|s| s.sequence);
        let prepared = contextual.decode::<Account>(&registry).unwrap();
        (used, prepared.stream().state().cents)
    }

    #[test]
    fn a_snapshot_under_another_rules_version_is_discarded() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v1", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (None, 100),
            "folded under v1, the current rules are v2: replayed from 0"
        );

        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v2", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (Some(1), 999),
            "a current one is used (and here shows its poison)"
        );
    }

    #[test]
    fn a_snapshot_of_another_state_type_is_discarded() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let foreign = Snapshot {
            sequence: 1,
            state: Some(pack(&shelf::ShelfState::default())),
            rules_version: "v2".to_string(),
        };
        store
            .write_snapshot(sref(&cover("account", 1)), &foreign)
            .ready()
            .unwrap();
        assert_eq!(load(&store, None), (None, 100));
    }

    #[test]
    fn a_snapshot_never_hides_part_of_the_window() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v2", 999))
            .ready()
            .unwrap();

        assert_eq!(
            load(&store, Some((1, ConflictResolution::AggregateHandles))),
            (None, 100),
            "the snapshot (at 1) would fold the window (from 1): not used"
        );
        assert_eq!(
            load(&store, Some((2, ConflictResolution::AggregateHandles))),
            (Some(1), 999),
            "at or below expected-1, it is"
        );
    }

    /// A snapshot at or past the head claims events the stream does not
    /// hold (one a purge left, or one written ahead of its events): not
    /// used, and the stream is read from its events. Used, the stream
    /// would resume past its head, its next events would not run on from
    /// it, and every later command on it would fail.
    #[test]
    fn a_snapshot_at_or_past_the_head_is_not_used() {
        use crate::next::toys::handle;
        let registry = registry();
        for sequence in [2, 5] {
            let store = MemoryStore::new();
            opened(&store, &registry, 1, 100);
            let ahead = Snapshot {
                sequence,
                ..poisoned("v2", 999)
            };
            store
                .write_snapshot(sref(&cover("account", 1)), &ahead)
                .ready()
                .unwrap();
            assert_eq!(
                load(&store, None),
                (None, 100),
                "a snapshot at {sequence}, the head at 2: read from the events"
            );
            for (n, head) in [(3, 3), (4, 4)] {
                let deposit = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(n);
                let handled = handle::<Account>(&store, &registry, deposit, &at(20))
                    .unwrap_or_else(|e| panic!("a snapshot at {sequence}, command {n}: {e}"));
                assert_eq!(handled.books()[0].next_sequence, head);
            }
        }
    }

    /// The kernel writes the snapshot from the state the router returns
    /// with a command's events (decision 14.40), stamped with the last
    /// event's sequence and the domain's rules version, under the default
    /// policy: the newest only, each replacing the last. A decision with no
    /// events writes none.
    #[test]
    fn the_state_a_command_returns_is_written_as_the_snapshot() {
        use crate::next::toys::handle;
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);
        let stored = || store.read_snapshot(sref(&stream)).ready().unwrap();
        let state = |snapshot: &Snapshot| {
            let any = snapshot.state.as_ref().expect("a snapshot holds a state");
            assert_eq!(any.type_url, "/toy.account.v1.AccountState");
            <account::AccountState as prost::Message>::decode(&any.value[..]).unwrap()
        };
        opened(&store, &registry, 1, 100);

        let snapshot = stored().expect("written with the deposit");
        assert_eq!(snapshot.sequence, 1, "the last event's");
        assert_eq!(snapshot.rules_version, "v2", "the domain's current rules");
        assert_eq!(state(&snapshot).cents, 100);
        assert!(registry.snapshot_usable("account", &snapshot));

        let deposit = command(stream.clone(), AccountCmd::Deposit(5).to_any()).id(3);
        handle::<Account>(&store, &registry, deposit, &at(12)).unwrap();
        let snapshot = stored().unwrap();
        assert_eq!(snapshot.sequence, 2, "the newest replaces the last");
        assert_eq!(state(&snapshot).cents, 105);

        let sweep_nothing = command(cover("account", 2), AccountCmd::Sweep.to_any()).id(4);
        opened(&store, &registry, 2, 0);
        let before = store
            .read_snapshot(sref(&cover("account", 2)))
            .ready()
            .unwrap();
        handle::<Account>(&store, &registry, sweep_nothing, &at(13)).unwrap();
        assert_eq!(
            store
                .read_snapshot(sref(&cover("account", 2)))
                .ready()
                .unwrap(),
            before,
            "no events, no snapshot"
        );
    }

    #[test]
    fn a_stream_snapshots_under_the_current_rules_version() {
        let registry = registry();
        let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
        let log = crate::next::MemoryDeadLetters::new();
        stream
            .execute(
                command(cover("shelf", 3), set("a", 1)),
                &registry,
                &at(1),
                &log,
                &mut [],
            )
            .ready()
            .unwrap();
        let snapshot = stream.snapshot(&registry).unwrap();
        assert_eq!(snapshot.sequence, 0);
        assert_eq!(snapshot.rules_version, "v1");
        assert_eq!(
            snapshot.state.as_ref().unwrap().type_url,
            "/toy.shelf.v1.ShelfState"
        );
        assert!(registry.snapshot_usable("shelf", &snapshot));
        assert!(Stream::<Shelf>::new(root(4), None)
            .unwrap()
            .snapshot(&registry)
            .is_none());
    }

    /// Ledger 1 after recording `cents`, executed in memory.
    fn recorded(cents: i64) -> Stream<crate::next::toys::Ledger> {
        use crate::next::MemoryDeadLetters;
        let registry = registry();
        let mut stream = Stream::<crate::next::toys::Ledger>::new(root(1), None).unwrap();
        let book = command(
            cover("ledger", 1),
            crate::next::toys::ledger::Record {
                source_sequence: 0,
                cents,
            }
            .to_any(),
        );
        stream
            .execute(
                book,
                &registry,
                &at(10),
                &MemoryDeadLetters::default(),
                &mut [],
            )
            .ready()
            .unwrap();
        stream
    }

    #[test]
    fn a_domain_typed_state_snapshots_as_its_wire_form_and_resumes_from_it() {
        let registry = registry();
        let stream = recorded(40);

        let snapshot = stream.snapshot(&registry).unwrap();
        assert_eq!(
            snapshot.state.as_ref().unwrap().type_url,
            "/toy.ledger.v1.LedgerSnapshot"
        );
        let resumed = Stream::<crate::next::toys::Ledger>::resume(
            cover("ledger", 1),
            &snapshot,
            None,
            &[],
            &registry,
        )
        .unwrap();
        assert_eq!(resumed.state(), stream.state());
        assert_eq!(resumed.state().total, 40);
    }

    #[test]
    fn a_snapshot_whose_wire_form_is_not_a_valid_state_is_unreadable() {
        use crate::next::KernelError;
        use angzarr_diagnostic::Diagnostic;
        let snapshot = Snapshot {
            sequence: 0,
            state: Some(pack(&crate::next::toys::ledger::LedgerSnapshot {
                next_source: 1,
                total: -5,
            })),
            rules_version: "v1".to_string(),
        };
        let error = Stream::<crate::next::toys::Ledger>::resume(
            cover("ledger", 1),
            &snapshot,
            None,
            &[],
            &registry(),
        )
        .err()
        .expect("a negative total is not a ledger");
        let KernelError::Unreadable { reason, .. } = error else {
            panic!("expected an unreadable snapshot, got {error:?}");
        };
        assert_eq!(reason.code(), "angzarr-embedded-next-payload-invalid");
    }
}

mod upcasting {
    //! The kernel upcasts every incoming payload before dispatch, through
    //! the domain's upcaster port (decision 14.39): the router is handed the
    //! current type only.
    use std::sync::{Arc, Mutex};

    use crate::next::angzarr_router::error::CodedError;
    use crate::next::angzarr_router::pb;
    use crate::next::angzarr_router::router::CommandHandler;
    use crate::next::toys::prelude::*;
    use crate::next::toys::{
        account, account_upcaster, at, command, cover, handlers, payloads, Account, AccountCmd,
        AccountEvent, Edit,
    };
    use crate::next::{pack, Failure, KernelError, Payload, Registry, Upcaster};

    /// The account's router table with no upcaster of its own, recording
    /// the type of every command it is handed.
    pub(crate) struct Seen {
        inner: Box<dyn crate::next::angzarr_router::router::DynCommandHandler>,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl CommandHandler for Seen {
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
            let sent = req
                .command
                .as_ref()
                .and_then(|book| book.page.as_ref())
                .and_then(|page| match &page.body {
                    Some(pb::command_page::Body::Command(c)) => c.payload.clone(),
                    Some(pb::command_page::Body::Fact(f)) => f.payload.clone(),
                    None => None,
                })
                .map(|payload| payload.type_url)
                .unwrap_or_default();
            self.seen.lock().unwrap().push(sent);
            self.inner.dispatch(req).await
        }
    }

    /// A registry whose account is [`Seen`], upcast through `upcaster`.
    fn seen_account(upcaster: Upcaster) -> (Registry, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let table =
            crate::next::route::table::<Account>(Arc::new(account::AccountState::default), false);
        let inner = handlers::<Account>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "account",
                Seen {
                    inner: Box::new(inner),
                    seen: Arc::clone(&seen),
                },
                crate::next::toys::descriptor_of::<Account>(
                    crate::next::v1::ComponentKind::Aggregate,
                    "v2",
                    &[],
                ),
            )
            .unwrap();
        registry.upcast_with("account", upcaster);
        (registry, seen)
    }

    fn any(
        store: &MemoryStore,
        registry: &Registry,
        book: crate::next::v1::CommandBook,
        seconds: i64,
    ) -> Result<crate::next::Handled, Failure<crate::next::Rejected>> {
        let log = crate::next::MemoryDeadLetters::new();
        crate::next::Kernel::new(registry, &log)
            .on_retryable(crate::next::toys::no_retries())
            .handle_any(store, book, &at(seconds), &mut ())
            .ready()
    }

    #[test]
    fn an_old_command_reaches_the_router_as_the_current_type() {
        let (registry, seen) = seen_account(account_upcaster());
        let store = MemoryStore::new();
        let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
        any(&store, &registry, open, 10).unwrap();

        let old = command(
            cover("account", 1),
            pack(&account::cmd::v0::Credit { euros: 2 }),
        )
        .id(2);
        let handled = any(&store, &registry, old, 11).unwrap();
        assert_eq!(
            payloads::<AccountEvent>(&handled.books()),
            [AccountEvent::Deposited(200)],
            "decided as the current type"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            [
                "/toy.account.cmd.v1.Open".to_string(),
                "/toy.account.cmd.v1.Deposit".to_string()
            ],
            "the router is handed the current type only"
        );
    }

    /// An upcaster sidecar that is down (decision 14.39, C-0681).
    struct Down;

    impl crate::next::UpcastPort for Down {
        fn upcast<'a>(
            &'a self,
            _: &'a crate::next::v1::UpcastRequest,
        ) -> crate::next::angzarr_router::send::BoxFuture<
            'a,
            Result<crate::next::v1::UpcastResponse, crate::next::UpcastError>,
        > {
            Box::pin(std::future::ready(Err(
                crate::next::UpcastError::Unavailable {
                    cause: angzarr_diagnostic::Report::new("toy-sidecar-down"),
                },
            )))
        }
    }

    /// A sidecar that cannot be reached delivers nothing that needs it, and
    /// its caller is answered to retry (C-0681).
    #[test]
    fn a_sidecar_that_is_down_is_retried_and_writes_nothing() {
        let (mut registry, seen) = seen_account(Upcaster::new("account-upcaster", "account"));
        registry.upcast_with("account", Down);
        let store = MemoryStore::new();
        let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
        let failure = any(&store, &registry, open, 10).err().unwrap();
        assert!(
            matches!(
                &failure,
                Failure::Kernel(KernelError::Upcast {
                    error: crate::next::UpcastError::Unavailable { .. },
                    ..
                })
            ),
            "{failure:?}"
        );
        assert_eq!(failure.recovery(), crate::next::Recovery::Retry);
        assert!(seen.lock().unwrap().is_empty(), "nothing delivered");
        assert!(pages(&store, &cover("account", 1)).is_empty());
    }

    #[test]
    fn an_unknown_type_after_upcasting_names_the_type_sent_and_the_type_upcast() {
        let upcaster = Upcaster::new("account-upcaster", "account")
            .rule::<account::cmd::v0::Credit, account::v9::Bonus>(|c| {
                Ok(account::v9::Bonus { cents: c.euros })
            });
        let (registry, seen) = seen_account(upcaster);
        let store = MemoryStore::new();
        let old = command(
            cover("account", 1),
            pack(&account::cmd::v0::Credit { euros: 2 }),
        )
        .id(2);
        let refused = any(&store, &registry, old, 11).err().unwrap();
        let Failure::Kernel(KernelError::Router(error)) = &refused else {
            panic!("the router's refusal: {refused:?}");
        };
        assert_eq!(error.code, "ROUTER_REASON_UNKNOWN_COMMAND_TYPE");
        let extra = |key: &str| error.extras.get(key).map(String::as_str);
        assert_eq!(
            extra("type_url"),
            Some("/toy.account.cmd.v0.Credit"),
            "as sent"
        );
        assert_eq!(
            extra("upcast_type_url"),
            Some("/toy.account.v9.Bonus"),
            "where the upcaster's chain ended"
        );
        assert_eq!(*seen.lock().unwrap(), ["/toy.account.v9.Bonus".to_string()]);

        // No rule applied: the two are the same.
        let (registry, _) = seen_account(Upcaster::new("account-upcaster", "account"));
        let bonus = command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 })).id(3);
        let refused = any(&store, &registry, bonus, 12).err().unwrap();
        let Failure::Kernel(KernelError::Router(error)) = &refused else {
            panic!("the router's refusal: {refused:?}");
        };
        let extra = |key: &str| error.extras.get(key).map(String::as_str);
        assert_eq!(extra("type_url"), Some("/toy.account.v9.Bonus"));
        assert_eq!(extra("upcast_type_url"), Some("/toy.account.v9.Bonus"));
    }

    /// The account's router table with no upcaster of its own, recording
    /// the type of every stored event it is handed.
    struct History {
        inner: Box<dyn crate::next::angzarr_router::router::DynCommandHandler>,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl CommandHandler for History {
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
            let types = (req.events.iter())
                .flat_map(|book| &book.pages)
                .filter_map(|page| page.payload.as_ref())
                .map(|payload| payload.type_url.clone());
            self.seen.lock().unwrap().extend(types);
            self.inner.dispatch(req).await
        }
    }

    /// A registry whose account is [`History`], a handler the kernel did
    /// not build (as one in another process is), upcast through
    /// `upcaster` as its sidecar; beside it the ledger.
    fn handled_account(upcaster: Upcaster) -> (Registry, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let table =
            crate::next::route::table::<Account>(Arc::new(account::AccountState::default), false);
        let inner = handlers::<Account>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "account",
                History {
                    inner: Box::new(inner),
                    seen: Arc::clone(&seen),
                },
                crate::next::toys::descriptor_of::<Account>(
                    crate::next::v1::ComponentKind::Aggregate,
                    "v2",
                    &[],
                ),
            )
            .unwrap();
        registry.upcast_with("account", upcaster);
        crate::next::toys::register::<crate::next::toys::Ledger>(
            &mut registry,
            crate::next::Upcaster::new("ledger-upcaster", "ledger"),
        )
        .unwrap();
        (registry, seen)
    }

    /// Account 1 as an older build wrote it: opened, then 2 euros credited
    /// under the old event type.
    fn old_history(store: &MemoryStore) {
        use crate::next::v1::{event_meta::Causation, EventMeta, EventPage};
        let page = |sequence: u64, payload| EventPage {
            payload: Some(payload),
            meta: Some(EventMeta {
                causation: Some(Causation::CommandId(crate::next::toys::command_id(
                    u128::from(sequence) + 1,
                ))),
                correlation_id: Some(crate::next::toys::command_id(u128::from(sequence) + 1)),
                recorded_at: Some(crate::next::toys::instant(sequence as i64)),
                rules_version: "v2".to_string(),
                ..EventMeta::default()
            }),
            sequence,
        };
        let log = [
            page(0, pack(&account::Opened {})),
            page(1, pack(&account::v0::Credited { euros: 2 })),
        ];
        store
            .write(Write::append(sref(&cover("account", 1)), 0, &log))
            .ready()
            .unwrap();
    }

    /// A stream loaded for a command reaches its handler in current shapes,
    /// through the domain's sidecar, whoever built the handler (C-0584).
    #[test]
    fn a_handlers_stored_events_reach_it_as_the_current_types() {
        let (registry, seen) = handled_account(account_upcaster());
        let store = MemoryStore::new();
        old_history(&store);
        let deposit = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(3);
        let handled = any(&store, &registry, deposit, 20);
        assert_eq!(
            *seen.lock().unwrap(),
            [
                "/toy.account.v1.Opened".to_string(),
                "/toy.account.v1.Deposited".to_string()
            ],
            "the handler is handed current types only"
        );
        assert_eq!(
            payloads::<AccountEvent>(&handled.unwrap().books()),
            [AccountEvent::Deposited(1)]
        );
    }

    /// A policy reading a stream's stored events is handed them in current
    /// shapes, through the source's sidecar, whoever built the source's
    /// handler (C-0586).
    #[test]
    fn a_policy_reads_a_handlers_stored_events_as_the_current_types() {
        use crate::next::toys::{ledger, ledger_root, no_retries, Bookkeeping};
        let (mut registry, _) = handled_account(account_upcaster());
        registry.policy(Bookkeeping);
        let store = MemoryStore::new();
        old_history(&store);
        let log = crate::next::MemoryDeadLetters::new();
        crate::next::Kernel::new(&registry, &log)
            .on_retryable(no_retries())
            .resume(&store, &at(30), &mut ())
            .ready()
            .unwrap();
        let ledger = crate::next::v1::Cover {
            domain: "ledger".to_string(),
            root: Some(ledger_root()),
            edition: None,
        };
        let recorded: Vec<(u64, i64)> = pages(&store, &ledger)
            .iter()
            .map(|page| {
                let payload = page.payload.as_ref().unwrap();
                let recorded =
                    <ledger::Recorded as prost::Message>::decode(&payload.value[..]).unwrap();
                (recorded.source_sequence, recorded.cents)
            })
            .collect();
        assert_eq!(recorded, [(0, 0), (1, 200)], "the credit booked in cents");
    }
}

mod admission {
    //! Admission is the kernel's own (decision 14.42): deduplication, the
    //! refusal fact's stream check and a process manager's start rule, by
    //! the input's type name after upcasting against the descriptor's
    //! `starting_types`.
    use std::sync::Arc;

    use angzarr_diagnostic::Diagnostic;

    use crate::next::angzarr_router::error::CodedError;
    use crate::next::angzarr_router::pb;
    use crate::next::angzarr_router::router::CommandHandler;
    use crate::next::toys::prelude::*;
    use crate::next::toys::{
        at, command, cover, fact, handlers, payloads, Account, AccountCmd, AccountEvent,
        AccountFact, Edit,
    };
    use crate::next::v1::DeliveryOutcome;
    use crate::next::{Failure, Handled, Payload, Registry};

    /// The account's table, as a process manager: its descriptor (in
    /// [`registry`]) has it started only by `Open`.
    struct AsProcessManager(Box<dyn crate::next::angzarr_router::router::DynCommandHandler>);

    impl CommandHandler for AsProcessManager {
        fn name(&self) -> &str {
            self.0.name()
        }
        fn kind(&self) -> pb::ComponentKind {
            pb::ComponentKind::ProcessManager
        }
        fn command_types(&self) -> Vec<String> {
            self.0.command_types()
        }
        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<pb::CommandHandlerResponse, CodedError> {
            self.0.dispatch(req).await
        }
    }

    fn registry() -> Registry {
        let table = crate::next::route::table::<Account>(
            Arc::new(crate::next::toys::account::AccountState::default),
            true,
        );
        let inner = handlers::<Account>()(table);
        let mut registry = Registry::new();
        registry
            .command_handler(
                "account",
                AsProcessManager(Box::new(inner)),
                crate::next::toys::descriptor_of::<Account>(
                    pb::ComponentKind::ProcessManager,
                    "v2",
                    &["toy.account.cmd.v1.Open"],
                ),
            )
            .unwrap();
        registry
    }

    fn any(
        store: &MemoryStore,
        registry: &Registry,
        book: crate::next::v1::CommandBook,
        seconds: i64,
    ) -> Result<Handled, Failure<crate::next::Rejected>> {
        let log = crate::next::MemoryDeadLetters::new();
        crate::next::Kernel::new(registry, &log)
            .on_retryable(crate::next::toys::no_retries())
            .handle_any(store, book, &at(seconds), &mut ())
            .ready()
    }

    #[test]
    fn a_non_starting_command_for_a_pm_with_no_instance_is_refused_by_the_kernel() {
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);

        let deposit = command(stream.clone(), AccountCmd::Deposit(5).to_any()).id(2);
        let refused = any(&store, &registry, deposit, 10).err().unwrap();
        let Failure::Rejected(rejected) = &refused else {
            panic!("a business refusal: {refused:?}");
        };
        assert_eq!(
            rejected.report().code(),
            "angzarr-embedded-next-pm-no-instance"
        );
        assert!(pages(&store, &stream).is_empty(), "nothing written");

        // A non-starting fact is skipped: applied, with no events.
        let paid = fact(
            stream.clone(),
            AccountFact::PaymentReceived(5).to_any(),
            "pi_1",
        );
        let skipped = any(&store, &registry, paid, 11).unwrap();
        assert_eq!(skipped.outcome().delivery, DeliveryOutcome::Applied as i32);
        assert!(skipped.books().is_empty());

        // A starting type starts it; then any input is dispatched.
        let open = || command(stream.clone(), AccountCmd::Open.to_any()).id(1);
        any(&store, &registry, open(), 12).unwrap();
        let deposit = command(stream.clone(), AccountCmd::Deposit(5).to_any()).id(3);
        let handled = any(&store, &registry, deposit, 13).unwrap();
        assert_eq!(
            payloads::<AccountEvent>(&handled.books()),
            [AccountEvent::Deposited(5)]
        );

        // The kernel deduplicates.
        let retry = any(&store, &registry, open(), 14).unwrap();
        assert_eq!(retry.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    }

    /// A refusal fact `paid` for account 1 whose causation names `trigger`'s
    /// stream, as the chain builds one.
    fn refusal_fact(registry: &Registry, trigger: u128) -> crate::next::Identified {
        use crate::next::toys::{command_id, correlation};
        let book = fact(
            cover("account", 1),
            AccountFact::PaymentReceived(5).to_any(),
            "refused",
        );
        let mut book = book;
        book.page.as_mut().unwrap().fact_origin = Some(crate::next::v1::FactOrigin {
            external_id: command_id(9).to_string(),
            description: "refused".to_string(),
        });
        let trigger = crate::next::v1::EventRef {
            stream: Some(cover("account", trigger)),
            sequence: 0,
        };
        crate::next::check::check_refusal(book, registry, trigger, correlation(1), command_id(9))
            .unwrap()
            .identify(&at(1))
    }

    #[test]
    fn a_refusal_fact_is_admitted_only_by_the_stream_its_causation_names() {
        use super::super::{admit, Admitted};
        let registry = registry();
        let addressed = refusal_fact(&registry, 1);
        let current = Ok(addressed.payload().clone());
        assert_eq!(
            admit(&registry, &addressed, &current, Vec::new(), true).unwrap(),
            Admitted::Dispatch
        );
        let misaddressed = refusal_fact(&registry, 2);
        let refused = admit(&registry, &misaddressed, &current, Vec::new(), true)
            .err()
            .unwrap();
        assert!(
            matches!(
                &refused,
                Failure::Kernel(crate::next::KernelError::Router(e))
                    if e.code == "COORDINATOR_REASON_REFUSAL_FACT_WRONG_STREAM"
                        && e.extras.get("domain").map(String::as_str) == Some("account")
            ),
            "{refused:?}"
        );
    }

    #[test]
    fn a_start_rule_on_a_payload_that_did_not_upcast_is_the_upcasts_failure() {
        use super::super::{admit, Admitted};
        use crate::next::{check, KernelError, UpcastError};
        let registry = registry();
        let deposit = command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(2);
        let identified = check(deposit, &registry).unwrap().identify(&at(1));
        let failed: Result<crate::next::Any, UpcastError> = Err(UpcastError::Rule {
            source: "toy.account.cmd.v1.Deposit".to_string(),
            error: angzarr_diagnostic::Report::new("toy-broken"),
        });
        let refused = admit(&registry, &identified, &failed, Vec::new(), false)
            .err()
            .unwrap();
        assert!(
            matches!(refused, Failure::Kernel(KernelError::Upcast { .. })),
            "{refused:?}"
        );
        // An instance exists: no start rule, so the type is not read here.
        assert_eq!(
            admit(&registry, &identified, &failed, Vec::new(), true).unwrap(),
            Admitted::Dispatch
        );
        // An aggregate has no start rule.
        let aggregate = crate::next::toys::registry();
        let identified = check(
            command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(2),
            &aggregate,
        )
        .unwrap()
        .identify(&at(1));
        assert_eq!(
            admit(&aggregate, &identified, &failed, Vec::new(), false).unwrap(),
            Admitted::Dispatch
        );
    }
}

mod rules_version {
    //! The rules version an answer reports (command_handler.proto,
    //! `CommandHandlerResponse.rules_version`): stamped in full, and refused
    //! as a handler bug when it is not the registered one (C-0558).
    use crate::next::angzarr_router::error::CodedError;
    use crate::next::angzarr_router::pb;
    use crate::next::toys::prelude::*;
    use crate::next::toys::{at, command, cover, Edit};
    use crate::next::{Failure, Recovery, Registry};

    /// A hand-written handler answering every command with one event,
    /// reporting `reported`.
    struct Reporting {
        reported: &'static str,
    }

    impl crate::next::angzarr_router::router::CommandHandler for Reporting {
        fn name(&self) -> &str {
            "order"
        }

        fn command_types(&self) -> Vec<String> {
            Vec::new()
        }

        async fn dispatch(
            &self,
            _: &pb::ContextualCommand,
        ) -> Result<pb::CommandHandlerResponse, CodedError> {
            Ok(pb::CommandHandlerResponse {
                conflict: None,
                events: Some(pb::EventBook {
                    pages: vec![pb::EventPage {
                        payload: Some(crate::next::Any {
                            type_url: "/orders.v1.ItemAdded".to_string(),
                            value: Vec::new(),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                rules_version: self.reported.to_string(),
            })
        }
    }

    /// `order`, registered at "1.4.2+build.7", answering `reported`.
    fn order(reported: &'static str) -> Registry {
        let mut registry = Registry::new();
        registry
            .command_handler(
                "order",
                Reporting { reported },
                pb::ComponentDescriptor {
                    name: "order".to_string(),
                    kind: pb::ComponentKind::Aggregate as i32,
                    rules_version: "1.4.2+build.7".to_string(),
                    state: Some(pb::StateDescriptor {
                        type_url: "/orders.v1.OrderState".to_string(),
                        options: Some(pb::StateOptions {
                            conflict_resolutions: vec![
                                pb::ConflictResolution::Strict as i32,
                                pb::ConflictResolution::AggregateHandles as i32,
                            ],
                        }),
                    }),
                    ..Default::default()
                },
            )
            .unwrap();
        registry
    }

    fn add_item() -> crate::next::v1::CommandBook {
        command(
            cover("order", 1),
            crate::next::Any {
                type_url: "/orders.v1.AddItem".to_string(),
                value: Vec::new(),
            },
        )
        .id(1)
    }

    /// C-0518: the version reported is stamped on every event, in full.
    #[test]
    fn every_event_records_the_reported_version_in_full() {
        let registry = order("1.4.2+build.7");
        let store = MemoryStore::new();
        let log = crate::next::MemoryDeadLetters::new();
        crate::next::Kernel::new(&registry, &log)
            .handle_any(&store, add_item(), &at(10), &mut ())
            .ready()
            .expect("applied");
        let written = pages(&store, &cover("order", 1));
        assert_eq!(
            written[0].meta.as_ref().map(|m| m.rules_version.as_str()),
            Some("1.4.2+build.7")
        );
    }

    /// C-0558: an answer reporting another version than the registered one
    /// (an empty one included) is a handler bug, refused INTERNAL, and
    /// nothing is persisted; it is dead-lettered, as every handler bug is.
    #[test]
    fn an_answer_reporting_another_version_is_refused_and_persists_nothing() {
        for reported in ["1.5.0+build.9", "1.4.2+build.8", ""] {
            let registry = order(reported);
            let store = MemoryStore::new();
            let log = crate::next::MemoryDeadLetters::new();
            let failure = crate::next::Kernel::new(&registry, &log)
                .handle_any(&store, add_item(), &at(10), &mut ())
                .ready()
                .err()
                .unwrap_or_else(|| panic!("{reported:?} is refused"));
            assert_eq!(failure.recovery(), Recovery::Bug, "{reported:?}");
            let Failure::Kernel(error) = &failure else {
                panic!("a kernel error: {failure:?}");
            };
            let coded = error.as_coded().expect("a spec key");
            assert_eq!(coded.code, "HOST_REASON_DESCRIPTOR_RULES_VERSION_MISMATCH");
            assert_eq!(
                coded.grpc,
                crate::next::angzarr_router::error::GrpcCode::Internal
            );
            assert_eq!(
                coded.extras.get("described").map(String::as_str),
                Some("1.4.2+build.7")
            );
            assert_eq!(
                coded.extras.get("reported").map(String::as_str),
                Some(reported)
            );
            assert_eq!(
                coded.extras.get("component").map(String::as_str),
                Some("order")
            );
            assert!(
                pages(&store, &cover("order", 1)).is_empty(),
                "nothing persisted"
            );
            assert_eq!(log.letters().len(), 1, "dead-lettered: {:?}", log.lines());
        }
    }
}

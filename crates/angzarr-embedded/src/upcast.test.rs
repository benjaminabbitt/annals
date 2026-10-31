//! Upcasting by type URL, for events and (discouraged) commands, in
//! registration order, with strict decoding.
use crate::toys::prelude::*;
use crate::v1::{EventMeta, EventPage, UpcastRequest};
use crate::{
    pack, ChainProblem, EventStore, Failure, KernelError, Registry, Stream, UpcastChainError,
    Upcaster,
};

use crate::toys::{
    account, account_upcaster, at, command, command_id, cover, handle, instant, opened, payloads,
    registry, registry_with_upcasters, Account, AccountEvent, Edit,
};

fn page(sequence: u64, payload: crate::Any) -> EventPage {
    EventPage {
        payload: Some(payload),
        meta: Some(EventMeta {
            causation: Some(crate::v1::event_meta::Causation::CommandId(command_id(
                sequence as u128,
            ))),
            correlation_id: Some(command_id(sequence as u128)),
            recorded_at: Some(instant(sequence as i64)),
            rules_version: "v2".to_string(),
            ..EventMeta::default()
        }),
        sequence,
    }
}

#[test]
fn upcasting_by_type_url_runs_the_chain_in_registration_order() {
    let upcaster = account_upcaster();
    assert_eq!(
        upcaster.source_types(),
        ["toy.account.cmd.v0.Credit", "toy.account.v0.Credited"]
    );
    let current = pack(&account::Deposited { cents: 200 });
    assert_eq!(
        upcaster.upcast_event(&pack(&account::v0::Credited { euros: 2 })),
        Ok(current.clone())
    );
    assert_eq!(upcaster.upcast_event(&current), Ok(current.clone()));
    let unknown = pack(&account::v9::Bonus { cents: 1 });
    assert_eq!(
        upcaster.upcast_event(&unknown),
        Ok(unknown.clone()),
        "other types pass through; strictness is at decode"
    );

    let old = page(
        u64::from(u32::MAX) + 1,
        pack(&account::v0::Credited { euros: 2 }),
    );
    let upcast = upcaster.upcast_page(&old).unwrap();
    assert_eq!(upcast.sequence, old.sequence, "sequence kept");
    assert_eq!(upcast.meta, old.meta, "meta kept");
    assert_eq!(upcast.payload, Some(current));

    let elsewhere = UpcastRequest {
        domain: "ledger".to_string(),
        events: vec![old.clone()],
    };
    assert_eq!(upcaster.dispatch(&elsewhere).unwrap().events, [old]);
}

#[test]
fn old_events_replay_through_the_upcaster() {
    let registry = registry_with_upcasters();
    let log = [
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::v0::Credited { euros: 3 })),
    ];
    let stream = Stream::<Account>::replay(cover("account", 1), &log, &registry).unwrap();
    assert_eq!(stream.state().cents, 300);
    assert_eq!(stream.events()[1].event(), &AccountEvent::Deposited(300));

    let store = MemoryStore::new();
    store
        .write(Write::append(sref(&cover("account", 1)), 0, &log))
        .ready()
        .unwrap();
    assert_eq!(
        pages(&store, &cover("account", 1)),
        log,
        "the store keeps what was written"
    );
}

#[test]
fn an_unknown_event_type_fails_replay_naming_it() {
    let log = [page(0, pack(&account::v9::Bonus { cents: 1 }))];
    let error = Stream::<Account>::replay(cover("account", 1), &log, &registry())
        .err()
        .unwrap();
    let KernelError::Unreadable {
        stream,
        sequence,
        reason,
    } = error
    else {
        panic!("never skipped: {error:?}");
    };
    assert_eq!((stream, sequence), (cover("account", 1), 0));
    assert!(format!("{reason:?}").contains("toy.account.v9.Bonus"));
}

#[test]
fn an_old_command_is_upcast_then_decided() {
    let registry = registry_with_upcasters();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);

    let old = command(
        cover("account", 1),
        pack(&account::cmd::v0::Credit { euros: 2 }),
    )
    .id(5);
    let handled = handle::<Account>(&store, &registry, old, &at(20)).unwrap();
    assert_eq!(
        payloads::<AccountEvent>(&handled.books()),
        [AccountEvent::Deposited(200)]
    );

    let without = crate::toys::registry();
    let old = command(
        cover("account", 1),
        pack(&account::cmd::v0::Credit { euros: 2 }),
    )
    .id(6);
    let refused = handle::<Account>(&store, &without, old, &at(21))
        .err()
        .unwrap();
    assert!(
        matches!(
            &refused,
            Failure::Kernel(KernelError::Router(e))
                if e.code == "ROUTER_REASON_UNKNOWN_COMMAND_TYPE"
                    && e.extras.get("type_url").map(String::as_str)
                        == Some("/toy.account.cmd.v0.Credit")
        ),
        "no upcaster path: the router's ROUTER_REASON_UNKNOWN_COMMAND_TYPE, naming the type URL: {refused:?}"
    );
}

#[test]
fn an_upcaster_is_checked_whole() {
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry
            .register::<Account>(upcaster, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|UpcastChainError { problem, .. }| problem)
            .collect()
    };

    assert_eq!(problems(account_upcaster()), []);
    let dead_end =
        Upcaster::new("u", "account").rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        });
    assert_eq!(
        problems(dead_end),
        [ChainProblem::DeadEnd {
            source: "toy.account.v0.Credited".to_string(),
            ends_at: "toy.account.v9.Bonus".to_string()
        }],
        "v9 is not a type the account decodes"
    );
    let twice = account_upcaster().rule::<account::v0::Credited, account::Deposited>(|c| {
        Ok(account::Deposited {
            cents: c.euros * 100,
        })
    });
    assert_eq!(
        problems(twice),
        [ChainProblem::Duplicate {
            source: "toy.account.v0.Credited".to_string()
        }]
    );
}

#[test]
fn an_upcaster_is_checked_for_order_and_untyped_rules_are_not_followed() {
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry
            .register::<Account>(upcaster, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|e| {
                assert_eq!(e.domain, "account");
                e.problem
            })
            .collect()
    };
    // v1 -> current registered before v0 -> v1: a v0 payload stops at v1.
    let out_of_order = Upcaster::new("u", "account")
        .rule::<account::v9::Bonus, account::Deposited>(|b| {
            Ok(account::Deposited { cents: b.cents })
        })
        .rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        });
    assert_eq!(
        problems(out_of_order),
        [
            ChainProblem::OutOfOrder {
                source: "toy.account.v9.Bonus".to_string(),
                produced_by: "toy.account.v0.Credited".to_string()
            },
            ChainProblem::DeadEnd {
                source: "toy.account.v0.Credited".to_string(),
                ends_at: "toy.account.v9.Bonus".to_string()
            }
        ]
    );
    let chained = Upcaster::new("u", "account")
        .rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        })
        .rule::<account::v9::Bonus, account::Deposited>(|b| {
            Ok(account::Deposited { cents: b.cents })
        });
    assert_eq!(problems(chained), [], "v0 -> v9 -> current finishes");
    let untyped = Upcaster::new("u", "account")
        .rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        })
        .on_event("toy.account.v9.Bonus", |any| Ok(any.clone()));
    assert_eq!(problems(untyped), [], "an untyped rule cannot be followed");
}

#[test]
fn an_upcast_rule_reports_its_own_failure_and_bytes_it_cannot_read() {
    let refusing =
        Upcaster::new("u", "account").rule::<account::v0::Credited, account::Deposited>(|_| {
            Err(angzarr_diagnostic::Report::new("toy-refused"))
        });
    let old = pack(&account::v0::Credited { euros: 2 });
    assert_eq!(
        refusing.upcast_event(&old),
        Err(crate::UpcastError::Rule {
            source: "toy.account.v0.Credited".to_string(),
            error: angzarr_diagnostic::Report::new("toy-refused")
        })
    );
    let garbled = crate::Any {
        type_url: old.type_url.clone(),
        value: vec![0x08],
    };
    assert!(matches!(
        account_upcaster().upcast_event(&garbled),
        Err(crate::UpcastError::Payload(
            crate::PayloadError::Decode { .. }
        ))
    ));

    let registry = {
        let mut registry = Registry::new();
        registry
            .register::<Account>(refusing, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
    };
    let store = MemoryStore::new();
    let failure = handle::<Account>(
        &store,
        &registry,
        command(cover("account", 1), old.clone()),
        &at(1),
    )
    .err()
    .unwrap();
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::Upcast { ref stream, .. }) if *stream == cover("account", 1)
    ));
    assert_eq!(failure.recovery(), crate::Recovery::Bug);

    let mut request = UpcastRequest {
        domain: "account".to_string(),
        events: vec![page(0, old), EventPage::default()],
    };
    assert_eq!(
        account_upcaster().dispatch(&request).unwrap().events,
        [
            page(0, pack(&account::Deposited { cents: 200 })),
            EventPage::default()
        ],
        "its own domain's pages upcast; a page with no payload kept"
    );
    request.events[1].payload = Some(pack(&account::v0::Credited { euros: 1 }));
    assert!(registry
        .upcaster("account")
        .unwrap()
        .dispatch(&request)
        .is_err());
}

#[test]
fn an_upcaster_names_itself_and_each_problem_once() {
    assert_eq!(account_upcaster().name(), "account-upcaster");
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry
            .register::<Account>(upcaster, crate::toys::handlers::<crate::toys::Account>())
            .unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.problem)
            .collect()
    };
    let same = Upcaster::new("u", "account").rule::<account::Deposited, account::Deposited>(Ok);
    assert_eq!(
        problems(same),
        [],
        "a rule onto its own type is not out of order"
    );
    let rule = |c: account::v0::Credited| Ok(account::v9::Bonus { cents: c.euros });
    let twice = Upcaster::new("u", "account")
        .rule::<account::v0::Credited, account::v9::Bonus>(rule)
        .rule::<account::v0::Credited, account::v9::Bonus>(rule);
    assert_eq!(
        problems(twice),
        [
            ChainProblem::Duplicate {
                source: "toy.account.v0.Credited".to_string()
            },
            ChainProblem::DeadEnd {
                source: "toy.account.v0.Credited".to_string(),
                ends_at: "toy.account.v9.Bonus".to_string()
            }
        ],
        "one chain, one dead end"
    );
}

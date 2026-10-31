use angzarr_proto::v1::event_meta::Causation;
use prost::Message;

use super::*;

fn uuid(n: u8) -> Uuid {
    Uuid {
        value: vec![n; 16].into(),
    }
}

/// A reaction's command as the chain identifies it: built directly,
/// since `check_policy_command` needs a registered domain and only the
/// meta is under test.
fn policy_command(trigger: EventRef) -> Identified {
    let command_id = ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap();
    Identified {
        cover: Cover {
            domain: "ledger".to_string(),
            root: Some(uuid(2)),
            edition: None,
        },
        root: uuid(2),
        body: Body::Command {
            payload: Any::default(),
        },
        command_id,
        correlation_id: uuid(8),
        correlation_sent: true,
        causation_id: Some(trigger),
        expected_sequence: None,
        conflict_resolution: None,
    }
}

/// A registered domain with nothing in it: enough for the parse.
mod empty {
    use std::convert::Infallible;

    use angzarr_proto::v1::{ConflictResolution, StateDescriptor, StateOptions};

    use crate::next::{Domain, Registry, State, Upcaster};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Nothing {}

    impl prost::Name for Nothing {
        const NAME: &'static str = "Nothing";
        const PACKAGE: &'static str = "test.v1";
    }

    impl State for Nothing {
        type Snapshot = Self;

        fn to_snapshot(&self) -> Self {
            self.clone()
        }

        fn from_snapshot(snapshot: Self) -> Result<Self, angzarr_diagnostic::Report> {
            Ok(snapshot)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/test.v1.Nothing".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }
        }
    }

    pub struct Ledger;

    impl Domain for Ledger {
        type State = Nothing;
        type Command = Infallible;
        type Fact = Infallible;
        type Event = Infallible;
        type Rejection = Infallible;
        const RULES_VERSION: &'static str = "v1";

        const NAME: &'static str = "ledger";

        fn apply(_: &mut Nothing, event: &Infallible) {
            match *event {}
        }
    }

    pub fn registry() -> Registry {
        let mut registry = Registry::new();
        registry
            .register::<Ledger>(Upcaster::new("u", "ledger"), |t| t)
            .unwrap();
        registry
    }
}

fn trigger() -> EventRef {
    EventRef {
        stream: Some(Cover {
            domain: "account".to_string(),
            root: Some(uuid(0xa7)),
            edition: None,
        }),
        sequence: 2,
    }
}

/// A policy's book as the kernel builds it: no ids, no handling.
fn book(body: command_page::Body) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "ledger".to_string(),
            root: Some(uuid(2)),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(body),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

fn command() -> command_page::Body {
    command_page::Body::Command(angzarr_proto::v1::Command {
        payload: Some(Any::default()),
    })
}

fn react(book: CommandBook) -> Result<Checked, Malformed> {
    check_policy_command(
        book,
        &empty::registry(),
        trigger(),
        "bookkeeping",
        1,
        uuid(8),
    )
}

#[test]
fn a_policys_book_is_identified_from_its_trigger() {
    let checked = react(book(command())).unwrap();
    assert!(checked.is_policy_command());
    assert!(!checked.body().is_fact());
    let id = checked.identify(&Sampled {
        now: Timestamp::default(),
        command_id: uuid(1),
    });
    assert_eq!(
        id.command_id(),
        &ids::policy_command_id(&trigger(), "bookkeeping", 1).unwrap()
    );
    assert_eq!(id.correlation_id(), &uuid(8));
    assert_eq!(id.causation_id(), Some(&trigger()));
    assert_eq!(id.domain(), "ledger");
    assert_eq!(id.root(), &uuid(2));
    assert_eq!(id.edition(), None);
    assert_eq!(id.to_book().handling, None, "none sent, none kept");

    let mut elsewhere = book(command());
    elsewhere.cover.as_mut().unwrap().edition = Some("what-if".to_string());
    let id = react(elsewhere).unwrap().identify(&Sampled {
        now: Timestamp::default(),
        command_id: uuid(1),
    });
    assert_eq!(id.edition(), Some("what-if"));
}

#[test]
fn a_policys_book_names_no_ids_no_fact_and_no_handling() {
    let with = |edit: fn(&mut CommandBook)| {
        let mut book = book(command());
        edit(&mut book);
        react(book).err()
    };
    assert_eq!(
        with(|b| b.page.as_mut().unwrap().command_id = Some(uuid(1))),
        Some(Malformed::IdOnPolicyCommand {
            field: "command_id"
        })
    );
    assert_eq!(
        with(|b| b.page.as_mut().unwrap().correlation_id = Some(uuid(1))),
        Some(Malformed::IdOnPolicyCommand {
            field: "correlation_id"
        })
    );
    assert_eq!(
        with(|b| b.page.as_mut().unwrap().causation_id = Some(trigger())),
        Some(Malformed::IdOnPolicyCommand {
            field: "causation_id"
        })
    );
    assert_eq!(
        with(|b| {
            b.handling = Some(Handling {
                expected_sequence: Some(0),
                conflict_resolution: 0,
            })
        }),
        Some(Malformed::HandlingOnPolicyCommand)
    );
    assert_eq!(
        with(|b| b.handling = Some(Handling::default())),
        None,
        "an empty handling is unset"
    );
    let fact = book(command_page::Body::Fact(angzarr_proto::v1::Fact {
        payload: Some(Any::default()),
    }));
    assert_eq!(react(fact).err(), Some(Malformed::FactFromPolicy));
    assert_eq!(
        check_policy_command(
            book(command()),
            &empty::registry(),
            EventRef::default(),
            "bookkeeping",
            0,
            uuid(8)
        )
        .err(),
        Some(Malformed::BadTrigger(UuidError::MissingTrigger))
    );
    assert_eq!(
        check_policy_command(
            book(command()),
            &empty::registry(),
            trigger(),
            "",
            0,
            uuid(8)
        )
        .err(),
        Some(Malformed::BadTrigger(UuidError::EmptyComponent))
    );
}

#[test]
fn a_policy_commands_events_carry_its_command_id_as_causation_and_no_trigger() {
    let trigger = EventRef {
        stream: Some(Cover {
            domain: "account".to_string(),
            root: Some(uuid(0xa7)),
            edition: None,
        }),
        sequence: 2,
    };
    let id = policy_command(trigger.clone());
    assert_eq!(id.causation_id(), Some(&trigger), "the command keeps it");

    let meta = id.meta(Timestamp::default(), "v1");
    assert_eq!(
        meta.causation,
        Some(Causation::CommandId(
            ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap()
        )),
        "the direct cause: the command"
    );
    let bytes = meta.encode_to_vec();
    assert!(
        !bytes.windows(16).any(|w| w == [0xa7; 16]),
        "nothing of the trigger is on the event"
    );
    assert_eq!(
        id.to_book().page.unwrap().causation_id,
        Some(trigger),
        "the book still names it, for transport and traces"
    );
}

mod parse {
    //! Step 1: one domain-blind parse checks everything, and what it accepts
    //! has non-optional accessors. These run: the parse is implemented.

    use crate::next::v1::{
        command_page, CommandBook, CommandPage, ConflictResolution, Cover, EventRef, FactOrigin,
        Handling, Uuid,
    };
    use crate::next::{check, ids, pack, Body, Malformed};

    use crate::next::toys::{
        account, command, correlation, cover, cover_in, fact, registry, root, set, Edit,
    };

    fn deposit(cents: i64) -> crate::next::Any {
        pack(&account::cmd::Deposit { cents })
    }

    fn payment(cents: i64) -> crate::next::Any {
        pack(&account::fact::PaymentReceived { cents })
    }

    #[test]
    fn a_checked_command_has_non_optional_accessors() {
        let book = command(cover("account", 1), deposit(5));
        let checked = check(book, &registry()).unwrap();

        assert_eq!(checked.cover(), &cover("account", 1));
        assert_eq!(checked.domain(), "account");
        assert_eq!(checked.root(), &root(1), "never an Option: checked present");
        assert_eq!(checked.root().value.len(), 16, "and 16 bytes");
        assert_eq!(checked.edition(), None, "main");
        assert_eq!(checked.payload(), &deposit(5));
        assert_eq!(
            checked.body(),
            &Body::Command {
                payload: deposit(5)
            }
        );
        assert!(!checked.body().is_fact());
        assert_eq!(checked.body().source(), None);
        assert_eq!(checked.command_id(), None, "assigned at identify");
        assert_eq!(checked.correlation_id(), None, "assigned at identify");
        assert_eq!(checked.expected_sequence(), None, "stamped at load");
        assert_eq!(checked.conflict_resolution(), None, "resolved at load");
        assert!(!checked.is_policy_command());
    }

    #[test]
    fn a_checked_fact_carries_its_source_in_its_body() {
        let book = fact(cover_in("account", 2, "what-if"), payment(70), "pi_1234");
        let checked = check(book, &registry()).unwrap();

        assert_eq!(checked.edition(), Some("what-if"));
        let source = FactOrigin {
            external_id: "pi_1234".to_string(),
            description: "toy payments webhook".to_string(),
        };
        assert_eq!(
            checked.body(),
            &Body::Fact {
                payload: payment(70),
                source: source.clone()
            }
        );
        assert_eq!(
            checked.body().source(),
            Some(&source),
            "a fact's source is never optional"
        );
        assert!(checked.body().is_fact());
        assert_eq!(checked.payload(), &payment(70));
    }

    #[test]
    fn a_checked_command_keeps_what_its_sender_chose() {
        let book = command(cover("shelf", 3), set("a", 1))
            .id(9)
            .correlated(4)
            .expecting(7, ConflictResolution::AggregateHandles);
        let checked = check(book, &registry()).unwrap();

        assert_eq!(
            checked.command_id(),
            Some(&crate::next::toys::command_id(9))
        );
        assert_eq!(checked.correlation_id(), Some(&correlation(4)));
        assert_eq!(checked.expected_sequence(), Some(7));
        assert_eq!(
            checked.conflict_resolution(),
            Some(ConflictResolution::AggregateHandles)
        );
    }

    #[test]
    fn an_empty_handling_is_unset() {
        let mut book = fact(cover("account", 1), payment(1), "pi_1");
        book.handling = Some(Handling::default());
        let checked = check(book, &registry()).expect("an empty handling on a fact is fine");
        assert_eq!(checked.expected_sequence(), None);

        let mut book = command(cover("account", 1), deposit(1));
        book.handling = Some(Handling {
            expected_sequence: None,
            conflict_resolution: ConflictResolution::Unspecified as i32,
        });
        assert_eq!(
            check(book, &registry()).unwrap().conflict_resolution(),
            None,
            "UNSPECIFIED is the configured default"
        );
    }

    #[test]
    fn a_fact_may_name_its_derived_command_id_and_nothing_else() {
        let mut book = fact(cover("account", 1), payment(1), "pi_1234");
        book.page_mut().command_id = Some(ids::fact_command_id("pi_1234"));
        assert!(check(book, &registry()).is_ok(), "an SDK may compute it");
    }

    #[test]
    fn commutative_is_accepted_where_the_state_declares_conflict_units() {
        let book =
            command(cover("shelf", 1), set("a", 1)).resolved_by(ConflictResolution::Commutative);
        assert_eq!(
            check(book, &registry()).unwrap().conflict_resolution(),
            Some(ConflictResolution::Commutative)
        );
        let book = command(cover("account", 1), deposit(1))
            .resolved_by(ConflictResolution::AggregateHandles);
        assert!(
            check(book, &registry()).is_ok(),
            "AGGREGATE_HANDLES is in every state's implicit base"
        );
    }

    /// A command page with this body, for building malformed cases.
    fn page(body: Option<command_page::Body>) -> Option<CommandPage> {
        Some(CommandPage {
            body,
            ..CommandPage::default()
        })
    }

    fn trigger() -> EventRef {
        EventRef {
            stream: Some(cover("shelf", 9)),
            sequence: 4,
        }
    }

    #[test]
    fn malformed_books_are_refused_naming_the_first_malformation() {
        let ok = || command(cover("account", 1), deposit(1));
        let ok_fact = || fact(cover("account", 1), payment(1), "pi_1");
        let short = |n: usize| Uuid {
            value: vec![0xab; n].into(),
        };
        let edit = |mut book: CommandBook, f: &dyn Fn(&mut CommandBook)| {
            f(&mut book);
            book
        };

        let cases: Vec<(&str, CommandBook, Malformed)> = vec![
            (
                "no cover",
                edit(ok(), &|b| b.cover = None),
                Malformed::NoCover,
            ),
            (
                "a domain nobody registered",
                command(cover("nowhere", 1), deposit(1)),
                Malformed::UnknownDomain {
                    domain: "nowhere".to_string(),
                },
            ),
            (
                "no root",
                edit(ok(), &|b| b.cover.as_mut().unwrap().root = None),
                Malformed::NoRoot,
            ),
            (
                "a root of 15 bytes",
                edit(ok(), &|b| b.cover.as_mut().unwrap().root = Some(short(15))),
                Malformed::BadRoot { len: 15 },
            ),
            (
                "a set empty edition: main is unset, never \"\"",
                command(
                    Cover {
                        edition: Some(String::new()),
                        ..cover("account", 1)
                    },
                    deposit(1),
                ),
                Malformed::EmptyEdition,
            ),
            (
                "no root and an empty edition: the first is the answer",
                command(
                    Cover {
                        root: None,
                        edition: Some(String::new()),
                        ..cover("account", 1)
                    },
                    deposit(1),
                ),
                Malformed::NoRoot,
            ),
            ("no page", edit(ok(), &|b| b.page = None), Malformed::NoPage),
            (
                "no body",
                edit(ok(), &|b| b.page = page(None)),
                Malformed::NoBody,
            ),
            (
                "no payload",
                edit(ok(), &|b| {
                    b.page = page(Some(command_page::Body::Command(Default::default())))
                }),
                Malformed::NoPayload,
            ),
            (
                "a command id of 3 bytes",
                edit(ok(), &|b| b.page_mut().command_id = Some(short(3))),
                Malformed::BadCommandId { len: 3 },
            ),
            (
                "an empty correlation id",
                edit(ok(), &|b| b.page_mut().correlation_id = Some(short(0))),
                Malformed::BadCorrelationId { len: 0 },
            ),
            (
                "a causation from outside the kernel",
                ok().caused_by(trigger()),
                Malformed::CausationFromOutside,
            ),
            (
                "a fact without its origin",
                edit(ok_fact(), &|b| b.page_mut().fact_origin = None),
                Malformed::FactWithoutOrigin,
            ),
            (
                "a fact with an empty external id",
                fact(cover("account", 1), payment(1), ""),
                Malformed::EmptyExternalId,
            ),
            (
                "a command with a fact origin",
                edit(ok(), &|b| {
                    b.page_mut().fact_origin = Some(FactOrigin::default())
                }),
                Malformed::CommandWithFactOrigin,
            ),
            (
                "a fact naming a command id other than its derivation",
                ok_fact().id(1),
                Malformed::FactCommandIdNotDerived,
            ),
            (
                "a fact with an expected sequence",
                ok_fact().expecting(3, ConflictResolution::Unspecified),
                Malformed::HandlingOnFact,
            ),
            (
                "a fact with a conflict resolution",
                ok_fact().resolved_by(ConflictResolution::Strict),
                Malformed::HandlingOnFact,
            ),
            (
                "a resolution this build does not know",
                edit(ok(), &|b| {
                    b.handling = Some(Handling {
                        expected_sequence: None,
                        conflict_resolution: 9,
                    })
                }),
                Malformed::UnknownConflictResolution { value: 9 },
            ),
            (
                "COMMUTATIVE on a state without conflict units",
                ok().expecting(0, ConflictResolution::Commutative),
                Malformed::UnsupportedConflictResolution {
                    domain: "account".to_string(),
                    requested: ConflictResolution::Commutative,
                },
            ),
        ];

        let registry = registry();
        for (case, book, malformed) in cases {
            assert_eq!(check(book, &registry), Err(malformed), "{case}");
        }
    }
}

mod identify {
    //! Step 2: ids are assigned once, after the check, as angzarr assigns them.
    //! These run: identification is implemented.

    use crate::next::v1::event_meta::Causation;
    use crate::next::v1::{EventMeta, FactOrigin};
    use crate::next::{check, ids, pack, Identified};

    use crate::next::toys::{
        account, at, command, command_id, correlation, cover, fact, instant, minted, registry, Edit,
    };

    fn identified(book: crate::next::v1::CommandBook, seconds: i64) -> Identified {
        check(book, &registry()).unwrap().identify(&at(seconds))
    }

    fn deposit() -> crate::next::Any {
        pack(&account::cmd::Deposit { cents: 5 })
    }

    #[test]
    fn a_client_command_without_ids_gets_the_minted_one_as_both() {
        let id = identified(command(cover("account", 1), deposit()), 30);
        assert_eq!(
            id.command_id(),
            &minted(30),
            "the application's UUIDv7, from Sampled"
        );
        assert_eq!(
            id.correlation_id(),
            &minted(30),
            "an absent correlation is the command id"
        );
        assert_eq!(id.causation_id(), None);
    }

    #[test]
    fn a_client_command_keeps_the_ids_it_sent() {
        let id = identified(
            command(cover("account", 1), deposit()).id(7).correlated(8),
            30,
        );
        assert_eq!(id.command_id(), &command_id(7), "reused verbatim on retry");
        assert_eq!(id.correlation_id(), &correlation(8));

        let id = identified(command(cover("account", 1), deposit()).id(7), 31);
        assert_eq!(
            id.correlation_id(),
            &command_id(7),
            "every retry of a client-keyed command lands in one correlation"
        );
    }

    #[test]
    fn a_fact_is_identified_by_its_external_id() {
        let payment = pack(&account::fact::PaymentReceived { cents: 70 });
        let id = identified(fact(cover("account", 1), payment.clone(), "pi_1234"), 30);
        let derived = "917f26ae-ea2e-58af-86de-77013a69617e"
            .parse()
            .expect("types.proto's worked example");
        assert_eq!(id.command_id(), &derived, "UUIDv5 of the external id");
        assert_eq!(id.command_id(), &ids::fact_command_id("pi_1234"));
        assert_eq!(
            id.correlation_id(),
            &derived,
            "and, unsupplied, its correlation"
        );
        assert_ne!(id.command_id(), &minted(30), "nothing minted for a fact");

        let id = identified(
            fact(cover("account", 1), payment, "pi_1234").correlated(3),
            30,
        );
        assert_eq!(id.command_id(), &derived);
        assert_eq!(id.correlation_id(), &correlation(3), "a sent one is kept");
    }

    #[test]
    fn every_event_of_a_command_carries_it_as_causation_and_no_actor() {
        let id = identified(command(cover("account", 1), deposit()).id(7), 30);
        assert_eq!(
            id.meta(instant(31), "v2"),
            EventMeta {
                causation: Some(Causation::CommandId(command_id(7))),
                fact: None,
                correlation_id: Some(command_id(7)),
                recorded_at: Some(instant(31)),
                rules_version: "v2".to_string(),
            }
        );

        let payment = pack(&account::fact::PaymentReceived { cents: 70 });
        let id = identified(fact(cover("account", 1), payment, "pi_9"), 30);
        assert_eq!(
            id.meta(instant(31), "v2").fact,
            Some(FactOrigin {
                external_id: "pi_9".to_string(),
                description: "toy payments webhook".to_string(),
            }),
            "a fact's events carry its source"
        );
    }

    #[test]
    fn an_identified_book_checks_again_and_identifies_the_same() {
        let registry = registry();
        for book in [
            command(cover("account", 1), deposit()),
            command(cover("account", 1), deposit()).id(2).correlated(3),
            fact(
                cover("account", 1),
                pack(&account::fact::PaymentReceived { cents: 1 }),
                "pi_2",
            ),
        ] {
            let first = check(book, &registry).unwrap().identify(&at(40));
            let again = check(first.to_book(), &registry)
                .expect("its ids are well formed, a fact's the derived one")
                .identify(&at(41));
            // Its correlation, filled in when unset, is set in the book: so
            // the re-read's is the sender's, which only a remote
            // coordinator's book tells apart (`to_remote_book`).
            assert_eq!(
                again.to_book(),
                first.to_book(),
                "a DLQ entry or an audit line re-reads as sent"
            );
        }
    }

    #[test]
    fn recorded_at_is_raised_to_the_streams_last_never_lowered() {
        use crate::next::{not_before, Timestamp};
        let later = Timestamp {
            seconds: 40,
            nanos: 5,
        };
        assert_eq!(not_before(None, instant(30)), instant(30));
        assert_eq!(
            not_before(Some(&later), instant(40)),
            later,
            "a clock step backwards never makes a later event look older"
        );
        assert_eq!(not_before(Some(&instant(20)), instant(30)), instant(30));
    }
}

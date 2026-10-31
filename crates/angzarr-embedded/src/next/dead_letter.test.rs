//! angzarr-embedded's dead-letter queue is a logger whose every line warns that it
//! almost certainly indicates a problem.
use super::{Cause, Component};
use crate::next::toys::prelude::*;
use crate::next::v1::{angzarr_dead_letter, AngzarrDeadLetter, ComponentKind, Diagnostic};
use crate::next::{
    check, dead_letter_line, pack, DeadLetterLog, Failure, Kernel, KernelError, MemoryDeadLetters,
    DEAD_LETTER_PREFIX,
};

use crate::next::toys::{account, at, command, cover, fact, registry, Account, Edit};

#[test]
fn a_dead_letter_line_starts_with_the_warning_and_names_the_command() {
    let book = check(
        command(
            cover("account", 1),
            pack(&account::cmd::Deposit { cents: 1 }),
        )
        .id(5),
        &registry(),
    )
    .unwrap()
    .identify(&at(30));
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedCommand(
            book.to_book(),
        )),
        reason: Some(Diagnostic {
            code: "angzarr-embedded-next-unhandled-handler-error".to_string(),
            args: [(
                "cause".to_string(),
                "ROUTER_REASON_HANDLER_PANICKED".to_string(),
            )]
            .into(),
        }),
        source_component: "account".to_string(),
        source_component_kind: ComponentKind::Aggregate as i32,
        ..AngzarrDeadLetter::default()
    };

    let line = dead_letter_line(&letter);
    assert!(
        line.starts_with(DEAD_LETTER_PREFIX),
        "an operator alerts on the prefix: {line}"
    );
    // A key to alert on, never English (decision 14.37).
    assert_eq!(DEAD_LETTER_PREFIX, "DEAD_LETTER");
    // The reason is the letter's Diagnostic: its code, then its args
    // (decision 14.37).
    assert!(
        line.contains(
            "account: angzarr-embedded-next-unhandled-handler-error cause=ROUTER_REASON_HANDLER_PANICKED"
        ),
        "{line}"
    );
    assert!(
        line.contains(&crate::next::toys::command_id(5).to_string()),
        "the command id, as display text: {line}"
    );

    let log = MemoryDeadLetters::new();
    log.write(&line);
    assert_eq!(log.lines(), std::slice::from_ref(&line));
    assert!(log.letters().is_empty(), "a bare line is no letter");

    let log = MemoryDeadLetters::new();
    log.letter(&letter);
    assert_eq!(
        log.letters(),
        std::slice::from_ref(&letter),
        "the letter, whole"
    );
    assert_eq!(log.lines(), std::slice::from_ref(&line), "and its line");

    // A log that keeps lines only writes a letter as its line.
    #[derive(Default)]
    struct Lines(std::sync::Mutex<Vec<String>>);
    impl DeadLetterLog for Lines {
        fn write(&self, line: &str) {
            self.0.lock().unwrap().push(line.to_string());
        }
    }
    let lines = Lines::default();
    lines.letter(&letter);
    assert_eq!(*lines.0.lock().unwrap(), [line]);
}

/// An edition removal's dead letter names its stream and the sequence of
/// its `EditionDeleted`, as failed events do.
#[test]
fn an_edition_removals_line_names_its_stream_and_sequence() {
    use crate::next::v1::EventPage;
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::FailedEditionRemoval(
            crate::next::v1::EventBook {
                cover: Some(cover("account", 1)),
                pages: vec![EventPage {
                    sequence: 4,
                    ..EventPage::default()
                }],
                ..crate::next::v1::EventBook::default()
            },
        )),
        source_component: "account".to_string(),
        ..AngzarrDeadLetter::default()
    };
    let line = dead_letter_line(&letter);
    assert!(line.contains(" sequence=4 stream=account/"), "{line}");
}

#[test]
fn a_panicking_ingest_aborts_the_chain_and_is_logged() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();

    let bad = fact(
        cover("account", 1),
        pack(&account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let failure = kernel
        .handle::<Account, _, _>(&store, bad, &at(30), &mut ())
        .ready()
        .err()
        .expect("a bug, never a business rejection");
    // The router catches the panic: a handler's bug, its cause
    // ROUTER_REASON_HANDLER_PANICKED (decision 14.37).
    assert!(
        matches!(
            &failure,
            Failure::Kernel(KernelError::Router(e))
                if e.extras.get("cause").map(String::as_str) == Some("ROUTER_REASON_HANDLER_PANICKED")
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), crate::next::Recovery::Bug);
    assert!(streams(&store).is_empty(), "nothing written");
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(DEAD_LETTER_PREFIX));
    assert!(lines[0].contains(&crate::next::ids::fact_command_id("pi_bad").to_string()));
}

/// A dead letter carries its error's code and args, never rendered
/// English: the operator's text is produced where the line is displayed.
#[test]
fn a_panicking_ingests_dead_letter_carries_its_code_not_english() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let bad = fact(
        cover("account", 1),
        pack(&account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let _ = kernel
        .handle::<Account, _, _>(&store, bad, &at(30), &mut ())
        .ready();
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(
        lines[0].contains(
            "account: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=ROUTER_REASON_HANDLER_PANICKED"
        ),
        "the spec's key, never the kernel's code: {}",
        lines[0]
    );
    let reason = crate::next::toys::wire(
        "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
        &[("cause", "ROUTER_REASON_HANDLER_PANICKED")],
    );
    crate::next::toys::assert_letter(
        &crate::next::toys::one_letter(log.letters()),
        crate::next::v1::ComponentKind::Aggregate,
        &reason,
        crate::next::toys::processing_failed(&reason, 0, false),
    );
    assert!(!lines[0].contains("panicked:"), "{}", lines[0]);
    assert!(
        !lines[0].contains("negative"),
        "the panic's text stays out: {}",
        lines[0]
    );
}

/// A dead letter's value that would not parse back is quoted (decision
/// 14.37's display rule), its reason's args and its metadata alike: one
/// empty, or holding whitespace, `=`, `"` or `\`, double-quoted with its
/// `"` and `\` escaped; any other value bare.
#[test]
fn a_dead_letters_ambiguous_value_is_quoted() {
    let pairs = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let letter = AngzarrDeadLetter {
        reason: Some(Diagnostic {
            code: "toy-store-down".to_string(),
            args: pairs(&[
                ("ext_err_txt", "database is locked"),
                ("empty", ""),
                ("path", "C:\\dir"),
                ("said", "say \"hi\""),
                ("seat", "3"),
            ]),
        }),
        metadata: pairs(&[("note", "k=v")]),
        source_component: "account".to_string(),
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(
        dead_letter_line(&letter),
        "DEAD_LETTER account: toy-store-down empty=\"\" ext_err_txt=\"database is locked\" \
         path=\"C:\\\\dir\" said=\"say \\\"hi\\\"\" seat=3 note=\"k=v\""
    );
}

/// A step whose retryable failure outlasted its retries is dead-lettered
/// with its command, its failure's own key and args as the reason and as
/// the processing failure's, transient, with how many retries it had; its
/// line shows the count.
#[test]
fn a_retried_steps_dead_letter_holds_its_command_failure_and_retry_count() {
    use crate::next::angzarr_router::error::{CodedError, RetryableStatus};
    use crate::next::toys::{processing_failed, wire};
    let book = deposit_book();
    let error = KernelError::Router(CodedError::retryable_bug(
        RetryableStatus::Unavailable,
        "toy-down",
        [("seat".to_string(), "3".to_string())],
    ));
    let letter = super::at_cap(
        angzarr_dead_letter::Payload::RejectedCommand(book.clone()),
        ACCOUNT,
        Cause::Kernel(&error),
        3,
    );
    let reason = wire(
        "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
        &[("cause", "toy-down"), ("cause.seat", "3")],
    );
    assert_eq!(
        letter.payload,
        Some(angzarr_dead_letter::Payload::RejectedCommand(book))
    );
    assert_eq!(letter.reason, Some(reason.clone()));
    assert_eq!(
        letter.rejection_details,
        Some(processing_failed(&reason, 3, true))
    );
    assert_eq!(letter.source_component, "account");
    let line = dead_letter_line(&letter);
    assert!(
        line.contains(
            "account: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-down cause.seat=3 retry_count=3 command_id="
        ),
        "{line}"
    );
}

/// The account aggregate, as a letter names it.
const ACCOUNT: Component<'static> = Component {
    name: "account",
    kind: ComponentKind::Aggregate,
};

/// A letter names the component that could not take its payload, and its
/// kind (`source_component`, `source_component_kind`), retried or not: a
/// policy, a projector, or a domain of the kind the registry holds it as.
#[test]
fn a_letter_names_its_component_and_its_kind() {
    let error = KernelError::ChainTooDeep { limit: 4 };
    let payload = || angzarr_dead_letter::Payload::RejectedCommand(deposit_book());
    for component in [
        ACCOUNT,
        Component::policy("bookkeeping"),
        Component::projector("balances"),
        Component::domain(&registry(), "account"),
    ] {
        for letter in [
            super::letter(payload(), component, Cause::Kernel(&error)),
            super::at_cap(payload(), component, Cause::Kernel(&error), 2),
        ] {
            assert_eq!(letter.source_component, component.name);
            assert_eq!(letter.source_component_kind(), component.kind);
        }
    }
    assert_eq!(Component::policy("bookkeeping").kind, ComponentKind::Policy);
    assert_eq!(
        Component::projector("balances").kind,
        ComponentKind::Projector
    );
    assert_eq!(
        Component::domain(&registry(), "account").kind,
        ComponentKind::Aggregate
    );
    assert_eq!(
        Component::domain(&registry(), "nowhere").kind,
        ComponentKind::Unspecified,
        "not registered here: no kind known"
    );
}

/// A command (5) to account 1, its ids assigned.
fn deposit_book() -> crate::next::v1::CommandBook {
    check(
        command(
            cover("account", 1),
            pack(&account::cmd::Deposit { cents: 1 }),
        )
        .id(5),
        &registry(),
    )
    .unwrap()
    .identify(&at(30))
    .to_book()
}

/// The letter for `cause`, about the deposit book.
fn letter_for(cause: Cause) -> AngzarrDeadLetter {
    super::letter(
        angzarr_dead_letter::Payload::RejectedCommand(deposit_book()),
        ACCOUNT,
        cause,
    )
}

/// Each sequence key's letter carries `sequence_mismatch`: the command's
/// expected sequence, and the head as `actual_sequence` (decision 14.48).
#[test]
fn a_sequence_keys_letter_carries_the_expected_sequence_and_the_head() {
    use crate::next::toys::{sequence_mismatch, wire};
    let stream = cover("account", 1);
    let stale = KernelError::Stale {
        stream: stream.clone(),
        expected: 2,
        actual: 5,
    };
    let letter = letter_for(Cause::Kernel(&stale));
    assert_eq!(
        letter.reason,
        Some(wire(
            "COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE",
            &[("actual", "5"), ("expected", "2")]
        ))
    );
    assert_eq!(letter.rejection_details, Some(sequence_mismatch(2, 5)));

    let ahead = KernelError::ExpectedSequenceAhead {
        stream: stream.clone(),
        expected: 7,
        head: 3,
    };
    let letter = letter_for(Cause::Kernel(&ahead));
    assert_eq!(
        letter.reason,
        Some(wire(
            "COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD",
            &[("expected", "7"), ("head", "3")]
        ))
    );
    assert_eq!(letter.rejection_details, Some(sequence_mismatch(7, 3)));

    let overlap = KernelError::Overlap {
        stream: stream.clone(),
        expected: 1,
        actual: 4,
        window_units: "a".to_string(),
        command_units: "a,b".to_string(),
    };
    let letter = letter_for(Cause::Kernel(&overlap));
    assert_eq!(
        letter.reason,
        Some(wire(
            "COORDINATOR_REASON_CONFLICT_OVERLAP",
            &[("command_units", "a,b"), ("window_units", "a")]
        ))
    );
    assert_eq!(letter.rejection_details, Some(sequence_mismatch(1, 4)));
}

/// A kernel error the spec has no key for keeps angzarr-embedded's code: a
/// fatal bug, `processing_failed`, never retried, not transient.
#[test]
fn a_kernel_error_with_no_spec_key_keeps_its_own_code() {
    use crate::next::toys::processing_failed;
    let error = KernelError::UnregisteredDomain {
        domain: "account".to_string(),
    };
    let letter = letter_for(Cause::Kernel(&error));
    let reason = crate::next::route::diagnostic(&error);
    assert_eq!(reason.code, "angzarr-embedded-next-unregistered-domain");
    assert_eq!(letter.reason, Some(reason.clone()));
    assert_eq!(
        letter.rejection_details,
        Some(processing_failed(&reason, 0, false))
    );
}

/// The two limits are keyed as the spec spells them (decision 14.49): a
/// chain deeper than the kernel's limit COORDINATOR_REASON_CHAIN_TOO_DEEP,
/// a decision past its step limit ROUTER_REASON_TOO_MANY_STEPS, each with
/// its `limit`; fatal bugs, `processing_failed`, never retried, not
/// transient.
#[test]
fn a_limits_letter_carries_its_spec_key_and_limit() {
    use crate::next::toys::{processing_failed, wire};
    for (error, reason) in [
        (
            KernelError::ChainTooDeep { limit: 4 },
            wire("COORDINATOR_REASON_CHAIN_TOO_DEEP", &[("limit", "4")]),
        ),
        (
            KernelError::TooManySteps {
                stream: cover("account", 1),
                limit: 16,
            },
            wire("ROUTER_REASON_TOO_MANY_STEPS", &[("limit", "16")]),
        ),
    ] {
        let letter = letter_for(Cause::Kernel(&error));
        assert_eq!(letter.reason, Some(reason.clone()), "{error:?}");
        assert_eq!(
            letter.rejection_details,
            Some(processing_failed(&reason, 0, false)),
            "{error:?}"
        );
    }
}

/// An unreachable remote handler's letter is keyed as the spec spells it
/// (decision 14.49): COORDINATOR_REASON_HANDLER_UNAVAILABLE, its domain and
/// cause.
#[test]
fn an_unavailable_handlers_letter_names_its_domain_and_cause() {
    use crate::next::toys::{processing_failed, wire};
    let error = KernelError::HandlerUnavailable {
        stream: cover("account", 1),
        cause: "not-running".to_string(),
    };
    let letter = letter_for(Cause::Kernel(&error));
    let reason = wire(
        "COORDINATOR_REASON_HANDLER_UNAVAILABLE",
        &[("cause", "not-running"), ("domain", "account")],
    );
    assert_eq!(letter.reason, Some(reason.clone()));
    assert_eq!(
        letter.rejection_details,
        Some(processing_failed(&reason, 0, false))
    );
}

/// A store that could not answer, and an upcaster sidecar that could not
/// be reached, are retryable failures, so each carries the spec's key for
/// when its retries are spent (decision 14.51):
/// COORDINATOR_REASON_STORE_UNAVAILABLE with the store's failure as its
/// `cause`, flattened, and COORDINATOR_REASON_UPCASTER_UNAVAILABLE with
/// its domain and cause; never angzarr-ports' or angzarr-embedded's code.
#[test]
fn a_retryable_store_or_sidecar_failures_letter_is_keyed_as_the_spec_spells_it() {
    use crate::next::toys::{processing_failed, wire};
    use angzarr_diagnostic::Report;
    let store = KernelError::Store(crate::next::StoreError::Unavailable(
        angzarr_ports::Cause::new(
            "angzarr-store-sqlite-unavailable",
            [("errno", angzarr_ports::Arg::Uint(5))],
        ),
    ));
    let sidecar = KernelError::Upcast {
        stream: cover("account", 1),
        error: crate::next::UpcastError::Unavailable {
            cause: Report::new("x-sidecar-down"),
        },
    };
    for (error, reason) in [
        (
            store,
            wire(
                "COORDINATOR_REASON_STORE_UNAVAILABLE",
                &[
                    ("cause", "angzarr-store-sqlite-unavailable"),
                    ("cause.errno", "5"),
                ],
            ),
        ),
        (
            sidecar,
            wire(
                "COORDINATOR_REASON_UPCASTER_UNAVAILABLE",
                &[("cause", "x-sidecar-down"), ("domain", "account")],
            ),
        ),
    ] {
        let letter = letter_for(Cause::Kernel(&error));
        assert_eq!(letter.reason, Some(reason.clone()), "{error:?}");
        assert_eq!(
            letter.rejection_details,
            Some(processing_failed(&reason, 0, false)),
            "{error:?}"
        );
    }
}

/// A book the kernel built that its check refused is keyed by the
/// coordinator's key where the spec has one, naming the book's domain and
/// its payload's type; otherwise by angzarr-embedded's code. Wrapped as a
/// kernel error, the same.
#[test]
fn a_malformed_kernel_books_letter_is_keyed_where_the_spec_has_a_key() {
    use crate::next::toys::{processing_failed, wire};
    use crate::next::Malformed;
    let naming = [
        ("command_type", "toy.account.cmd.v1.Deposit"),
        ("domain", "account"),
    ];
    for (malformed, reason) in [
        (
            Malformed::HandlingOnPolicyCommand,
            wire("COORDINATOR_REASON_POLICY_COMMAND_WITH_HANDLING", &naming),
        ),
        (
            Malformed::NoRoot,
            wire("COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT", &naming),
        ),
        (
            Malformed::BadRoot { len: 3 },
            wire("COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT", &naming),
        ),
        (
            Malformed::RefusalMisaddressed,
            wire(
                "COORDINATOR_REASON_REFUSAL_FACT_WRONG_STREAM",
                &[("domain", "account")],
            ),
        ),
        (
            Malformed::FactFromPolicy,
            wire("COORDINATOR_REASON_POLICY_COMMAND_IS_FACT", &naming),
        ),
        (
            Malformed::CommandWithFactOrigin,
            wire("COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN", &naming),
        ),
        (
            Malformed::IdOnPolicyCommand {
                field: "command_id",
            },
            wire(
                "COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY",
                &[naming[0], naming[1], ("field", "command_id")],
            ),
        ),
        (
            Malformed::IdOnPolicyCommand {
                field: "causation_id",
            },
            wire(
                "COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY",
                &[naming[0], naming[1], ("field", "causation_id")],
            ),
        ),
        (
            Malformed::IdOnPolicyCommand {
                field: "correlation_id",
            },
            wire(
                "COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION",
                &naming,
            ),
        ),
        (
            Malformed::UnknownDomain {
                domain: "account".to_string(),
            },
            wire("COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN", &naming),
        ),
        (
            Malformed::NoBody,
            wire("angzarr-embedded-next-malformed-no-body", &[]),
        ),
    ] {
        let letter = letter_for(Cause::Malformed(&malformed));
        assert_eq!(letter.reason, Some(reason.clone()), "{malformed:?}");
        assert_eq!(
            letter.rejection_details,
            Some(processing_failed(&reason, 0, false)),
            "{malformed:?}"
        );
        let wrapped = KernelError::Malformed(malformed.clone());
        assert_eq!(
            letter_for(Cause::Kernel(&wrapped)).reason,
            Some(reason),
            "{malformed:?}"
        );
    }
    // A fact's payload type names it too.
    let fact = check(
        crate::next::toys::fact(
            cover("account", 1),
            pack(&account::fact::PaymentReceived { cents: 1 }),
            "pi_1",
        ),
        &registry(),
    )
    .unwrap()
    .identify(&at(30))
    .to_book();
    let letter = super::letter(
        angzarr_dead_letter::Payload::RejectedCommand(fact),
        ACCOUNT,
        Cause::Malformed(&Malformed::NoRoot),
    );
    assert_eq!(
        letter
            .reason
            .unwrap()
            .args
            .get("command_type")
            .map(String::as_str),
        Some("toy.account.fact.v1.PaymentReceived")
    );
    // Events name no command: empty.
    let letter = super::letter(
        angzarr_dead_letter::Payload::RejectedEvents(crate::next::v1::EventBook {
            cover: Some(cover("account", 1)),
            ..crate::next::v1::EventBook::default()
        }),
        ACCOUNT,
        Cause::Malformed(&Malformed::NoRoot),
    );
    assert_eq!(
        letter.reason,
        Some(wire(
            "COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT",
            &[("command_type", ""), ("domain", "")]
        ))
    );
}

/// A refusal's letter is keyed as the refusing handler answered, never
/// angzarr-embedded's code for it; a typed one carries its object whole
/// in `command_refused`, an untyped one `processing_failed`.
#[test]
fn a_refusals_letter_is_keyed_as_answered() {
    use crate::next::angzarr_router::error::{BusinessStatus, CodedError};
    use crate::next::toys::{processing_failed, wire};
    use crate::next::v1::angzarr_dead_letter::RejectionDetails;
    use crate::next::v1::CommandRefusedDetails;
    let untyped = crate::next::route::Rejected::of(&CodedError::rejection(
        BusinessStatus::FailedPrecondition,
        "toy-ledger-too-large",
        [("cents".to_string(), "2000".to_string())],
    ));
    let letter = letter_for(Cause::Refused(&untyped));
    let reason = wire("toy-ledger-too-large", &[("cents", "2000")]);
    assert_eq!(letter.reason, Some(reason.clone()));
    assert_eq!(
        letter.rejection_details,
        Some(processing_failed(&reason, 0, false))
    );

    let object = pack(&account::cmd::Deposit { cents: 9 });
    let typed = crate::next::route::Rejected::of(&CodedError::refused(object.clone()));
    let letter = letter_for(Cause::Refused(&typed));
    assert_eq!(
        letter.reason,
        Some(wire(
            "ROUTER_REASON_COMMAND_REFUSED",
            &[("refusal_type", "toy.account.cmd.v1.Deposit")]
        ))
    );
    assert_eq!(
        letter.rejection_details,
        Some(RejectionDetails::CommandRefused(CommandRefusedDetails {
            refusal_type: "toy.account.cmd.v1.Deposit".to_string(),
            refusal: Some(object),
        }))
    );
}

/// A line shows a letter's retry count only where it was retried.
#[test]
fn a_line_shows_no_retry_count_for_a_letter_never_retried() {
    let error = KernelError::ChainTooDeep { limit: 4 };
    let line = dead_letter_line(&letter_for(Cause::Kernel(&error)));
    assert!(!line.contains("retry_count"), "{line}");
}

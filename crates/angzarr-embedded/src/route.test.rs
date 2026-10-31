use prost_types::Any;

use super::{is_nothing, NOTHING};
use crate::v1::EventPage;

/// A fact's record of nothing is known by its type URL alone.
#[test]
fn a_record_of_nothing_is_known_by_its_type_url() {
    let page = |type_url: &str| EventPage {
        payload: Some(Any {
            type_url: type_url.to_string(),
            value: Vec::new(),
        }),
        ..EventPage::default()
    };
    assert!(is_nothing(&page(NOTHING)));
    assert!(!is_nothing(&page("/toy.account.v1.Opened")));
    assert!(!is_nothing(&EventPage::default()));
}

/// A fold that fails on a persisted event says why as its cause, a key and
/// args (decision 14.37: `FoldFailure.cause`, flattened as `cause` and
/// `cause.<arg>`), never as text: here the kernel's own payload error for
/// bytes that do not decode as the event's type.
#[test]
fn a_fold_that_fails_carries_its_cause_as_a_key_and_args() {
    use crate::toys::{account, command, cover, registry, Ready};
    use angzarr_router::error::RouterReason;
    use angzarr_router::pb::ContextualCommand;

    let registry = registry();
    let stream = cover("account", 1);
    let mut corrupt = crate::pack(&account::Deposited { cents: 1 });
    corrupt.value = vec![0xff, 0xff, 0xff];
    let type_url = corrupt.type_url.clone();
    let request = ContextualCommand {
        events: Some(crate::v1::EventBook {
            cover: Some(stream.clone()),
            pages: vec![EventPage {
                sequence: 0,
                payload: Some(corrupt),
                meta: None,
            }],
            snapshot: None,
            next_sequence: 1,
        }),
        command: Some(command(
            stream,
            crate::pack(&account::cmd::Deposit { cents: 1 }),
        )),
        ..ContextualCommand::default()
    };
    let error = registry
        .handler("account")
        .expect("registered")
        .dispatch(&request)
        .ready()
        .expect_err("the persisted event does not fold");
    assert_eq!(error.code, RouterReason::FoldFailed.as_str_name());
    let extra = |key: &str| error.extras.get(key).map(String::as_str);
    assert_eq!(
        extra("cause"),
        Some("angzarr-embedded-next-payload-decode"),
        "{error:?}"
    );
    assert_eq!(
        extra("cause.type_url"),
        Some(type_url.as_str()),
        "{error:?}"
    );
}

/// A handler's refusal goes out under whichever of the six business
/// statuses it gave, typed or not, and `Rejected::status` reads that same
/// status back: an untyped refusal under its own code and args, a typed
/// one keyed ROUTER_REASON_COMMAND_REFUSED with its object beside.
#[test]
fn a_refusal_goes_out_and_reads_back_under_each_business_status() {
    use angzarr_diagnostic::{Diagnostic, Report};
    use angzarr_router::error::{BusinessStatus, GrpcCode, HandlerError};

    use super::{refused, Rejected};

    let coded = |error: HandlerError| match error {
        HandlerError::Coded(coded) => coded,
        other => panic!("coded, not {other:?}"),
    };
    let object = Any {
        type_url: "type.googleapis.com/toy.meter.v1.MeterSealed".to_string(),
        value: Vec::new(),
    };
    for status in [
        BusinessStatus::FailedPrecondition,
        BusinessStatus::InvalidArgument,
        BusinessStatus::NotFound,
        BusinessStatus::AlreadyExists,
        BusinessStatus::OutOfRange,
        BusinessStatus::PermissionDenied,
    ] {
        let report = Report::new("toy-meter-low").arg("reading", 3i64);

        let untyped = coded(refused(&report, None, status));
        assert_eq!(untyped.grpc, GrpcCode::from(status), "{status:?}");
        assert_eq!(untyped.code, "toy-meter-low", "{status:?}");
        assert_eq!(
            untyped.extras.get("reading").map(String::as_str),
            Some("3"),
            "{status:?}"
        );
        let read = Rejected::of(&untyped);
        assert_eq!(read.status(), status);
        assert_eq!(read.code(), "toy-meter-low");
        assert_eq!(read.refusal_object(), None);

        let typed = coded(refused(&report, Some(object.clone()), status));
        assert_eq!(typed.grpc, GrpcCode::from(status), "{status:?}");
        assert_eq!(typed.code, "ROUTER_REASON_COMMAND_REFUSED", "{status:?}");
        let read = Rejected::of(&typed);
        assert_eq!(read.status(), status);
        assert_eq!(read.code(), "angzarr-embedded-next-command-refused");
        assert_eq!(read.refusal_object(), Some(&object));
    }
}

/// An untyped refusal reaches a refused policy's OnRefused as
/// `RefusalResponse.diagnostic`, the rejection's ErrorInfo reason as its
/// code: the key it went out under, the handler's own or the spec's (a
/// process manager's `COORDINATOR_REASON_PM_NO_INSTANCE`), never
/// angzarr-embedded's code for it; its metadata the args.
#[test]
fn an_untyped_refusal_reaches_on_refused_under_the_key_it_went_out_under() {
    use angzarr_proto::v1::refusal_response::Reason;
    use angzarr_proto::v1::Diagnostic;
    use angzarr_router::error::{BusinessStatus, CodedError};

    use super::Rejected;

    for (key, arg) in [
        ("COORDINATOR_REASON_PM_NO_INSTANCE", "type_url"),
        ("toy-meter-low", "reading"),
    ] {
        let rejected = Rejected::of(&CodedError::rejection(
            BusinessStatus::FailedPrecondition,
            key,
            [(arg.to_string(), "3".to_string())],
        ));
        assert_eq!(
            rejected.response().reason,
            Some(Reason::Diagnostic(Diagnostic {
                code: key.to_string(),
                args: [(arg.to_string(), "3".to_string())].into(),
            })),
            "{key}"
        );
    }
}

/// A marked-up handler's retryable bug goes out under exactly the status
/// it picked, ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its report as the cause; its
/// immediate return as ABORTED under the report's own code and args.
#[test]
fn a_retryable_bug_and_an_immediate_return_go_out_under_their_status() {
    use angzarr_diagnostic::Report;
    use angzarr_router::error::{GrpcCode, HandlerError, RetryableStatus};

    use super::failed;
    use crate::markup::{Failed, Rejection};

    let coded = |error: HandlerError| match error {
        HandlerError::Coded(coded) => coded,
        other => panic!("coded, not {other:?}"),
    };
    let report = || Report::new("toy-meter-offline").arg("seat", 3i64);
    for status in [
        RetryableStatus::Unavailable,
        RetryableStatus::DeadlineExceeded,
        RetryableStatus::ResourceExhausted,
    ] {
        let out = coded(failed::<Report>(
            Failed::<Rejection<Report>>::retryable_bug(status, &report()),
        ));
        assert_eq!(out.grpc, GrpcCode::from(status), "{status:?}");
        assert_eq!(
            out.code, "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
            "{status:?}"
        );
        assert_eq!(
            out.extras.get("cause").map(String::as_str),
            Some("toy-meter-offline"),
            "{status:?}"
        );
        assert_eq!(
            out.extras.get("cause.seat").map(String::as_str),
            Some("3"),
            "{status:?}"
        );
    }

    let out = coded(failed::<Report>(Failed::<Rejection<Report>>::immediate(
        &report(),
    )));
    assert_eq!(out.grpc, GrpcCode::Aborted);
    assert_eq!(out.code, "toy-meter-offline");
    assert_eq!(out.extras.get("seat").map(String::as_str), Some("3"));
    assert_eq!(out.extras.get("cause"), None);
}

/// What this thread holds allocated, net: every allocation counted against
/// the thread that made it, every free against the thread that freed it.
/// The test binary's allocator: the system's, counting.
mod counted {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        // Const-initialized and without a destructor: reading it never
        // allocates, so the allocator may.
        static LIVE: Cell<isize> = const { Cell::new(0) };
    }

    pub struct Counting;

    fn count(delta: isize) {
        let _ = LIVE.try_with(|live| live.set(live.get() + delta));
    }

    // SAFETY: every call is the system allocator's, with its arguments
    // unchanged; the count beside it touches no allocation.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count(layout.size() as isize);
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            count(-(layout.size() as isize));
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            count(layout.size() as isize);
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            count(new_size as isize - layout.size() as isize);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    /// The bytes this thread holds allocated, net, so far.
    pub fn live() -> isize {
        LIVE.with(Cell::get)
    }
}

/// A3: the codes and arg names a handler answers are held while its error
/// is, and freed with it. A child answers arbitrary strings, so a stream of
/// distinct codes must not grow what the process holds: each error, its
/// refusal, its diagnostic and its display, made and dropped, leaves
/// nothing behind.
#[test]
fn a_stream_of_distinct_child_codes_leaks_nothing() {
    use angzarr_diagnostic::Diagnostic;
    use angzarr_router::error::{BusinessStatus, CodedError};

    let answered = |n: usize| {
        let refusal = CodedError::rejection(
            BusinessStatus::FailedPrecondition,
            &format!("CHILD_REFUSED_{n}"),
            [(format!("seat_{n}"), n.to_string())],
        );
        let rejected = super::Rejected::of(&refusal);
        let shown = (
            rejected.to_string(),
            rejected.code().len(),
            rejected.args().len(),
        );
        let bug = super::internal(
            &format!("CHILD_BROKE_{n}"),
            [(format!("table_{n}"), n.to_string())],
        );
        let routed = crate::KernelError::Router(bug.clone());
        let routed = (routed.to_string(), routed.code().len(), routed.args().len());
        let failure = super::policy_failed(&angzarr_diagnostic::Report::new("toy-policy-broke"));
        drop((shown, routed, failure));
        let failed = super::internal(
            super::POLICY_FAILED,
            [
                ("policy-reason".to_string(), format!("CHILD_POLICY_{n}")),
                (format!("hand_{n}"), n.to_string()),
            ],
        );
        let reported = super::policy_failure(&failed).map(|r| r.to_string());
        drop(reported);
    };
    // Whatever is built once, on first use, is built before counting.
    answered(0);
    let before = counted::live();
    for n in 1..=2_000 {
        answered(n);
    }
    let held = counted::live() - before;
    assert!(
        held < 1_024,
        "{held} bytes still held after 2000 distinct codes were answered and dropped"
    );
}

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use angzarr_router::error::GrpcCode;
use angzarr_router::pb;
use angzarr_router::router::CommandHandler;
use prost_types::Any;

use super::*;
use crate::config::ComponentConfig;
use crate::error::HandshakeError;
use crate::fake_child::{self, config, domain, mode};
use crate::remote::is_retryable;

fn command(domain: &str, type_url: &str) -> pb::ContextualCommand {
    pb::ContextualCommand {
        command: Some(pb::CommandBook {
            cover: Some(pb::Cover {
                domain: domain.to_string(),
                ..Default::default()
            }),
            page: Some(pb::CommandPage {
                body: Some(pb::command_page::Body::Command(pb::Command {
                    payload: Some(Any {
                        type_url: type_url.to_string(),
                        value: Vec::new(),
                    }),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A future run to its end on a runtime of its own (with its timer), as
/// the kernel's caller runs it: `handler.dispatch(&c).awaited()`.
trait Awaited: std::future::Future + Sized {
    fn awaited(self) -> Self::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(self)
    }
}

impl<F: std::future::Future> Awaited for F {}

fn player(mode: &str) -> ComponentConfig {
    domain("player", "player", mode)
}

fn started(domains: Vec<ComponentConfig>) -> Host {
    Host::start(config(domains)).expect("the host starts")
}

/// The fake's `pid-<n>` rules version, from its descriptor.
fn pid_of(handler: &RemoteCommandHandler) -> String {
    handler.descriptor().rules_version.clone()
}

/// Whether the process `pid-<n>` names is running (a zombie is not).
fn alive(pid: &str) -> bool {
    let pid = pid.strip_prefix("pid-").unwrap();
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(") ")
            .is_some_and(|(_, rest)| !rest.starts_with('Z'))
    })
}

/// Describe's descriptor is the handler's: its name, its kind.
#[test]
fn start_describes_each_remote_domain() {
    let host = started(vec![player(mode::SERVE)]);
    let handlers = host.handlers();
    assert_eq!(handlers.len(), 1);
    let handler = &handlers[0];
    assert_eq!(handler.name(), "player");
    assert_eq!(handler.kind(), pb::ComponentKind::Aggregate);
    let descriptor = handler.descriptor();
    assert_eq!(descriptor.name, "player");
    assert_eq!(descriptor.kind, pb::ComponentKind::Aggregate as i32);
    assert!(descriptor.rules_version.starts_with("pid-"));
    assert_eq!(handler.command_types(), Vec::<String>::new());
    assert_eq!(
        format!("{handler:?}"),
        "RemoteCommandHandler { domain: \"player\", .. }"
    );
    assert_eq!(host.handler("player").unwrap().name(), "player");
    assert!(host.handler("table").is_none());
    host.shutdown();
}

/// Every domain the config names, each with its own child, in order; the
/// in-process ones are not launched and get no handler.
#[test]
fn in_process_domains_are_left_to_the_caller() {
    let table = ComponentConfig {
        name: "table".to_string(),
        language: "rust".to_string(),
        kind: crate::config::ComponentKindConfig::Domain,
        upcasts: None,
        launch: None,
        entry: Vec::new(),
        dir: None,
        env: Default::default(),
        expose: Default::default(),
    };
    let host = started(vec![
        table,
        player(mode::SERVE),
        domain("dealer", "dealer", mode::SERVE),
    ]);
    let names: Vec<String> = host
        .handlers()
        .iter()
        .map(|h| h.name().to_string())
        .collect();
    assert_eq!(names, ["player", "dealer"]);
    let (player, dealer) = (
        host.handler("player").unwrap(),
        host.handler("dealer").unwrap(),
    );
    assert_ne!(pid_of(&player), pid_of(&dealer));
    host.shutdown();
}

/// `Handle` on the wire: the command reaches the child and its answer comes
/// back whole.
#[test]
fn a_command_round_trips_through_the_child() {
    let host = started(vec![player(mode::SERVE)]);
    let handler = host.handler("player").unwrap();
    let response = handler
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .expect("dispatched");
    let events = response.events.expect("events");
    assert_eq!(events.cover.unwrap().domain, "player");
    assert_eq!(response.rules_version, pid_of(&handler));
    host.shutdown();
}

/// The kernel may dispatch from inside its own tokio runtime: the call must
/// neither panic nor deadlock there.
#[test]
fn dispatch_works_inside_a_tokio_runtime() {
    let host = started(vec![player(mode::SERVE)]);
    let handler = host.handler("player").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let response = runtime.block_on(async {
        handler
            .dispatch(&command("player", "/examples.blackjack.SitDown"))
            .await
    });
    assert!(response.is_ok(), "{response:?}");
    host.shutdown();
}

/// A child's FAILED_PRECONDITION, with its typed refusal in the details,
/// is the router's refusal.
#[test]
fn a_refusal_comes_back_as_the_routers_refusal() {
    let host = started(vec![player(mode::SERVE)]);
    let refused = host
        .handler("player")
        .unwrap()
        .dispatch(&command("player", "/examples.blackjack.Refuse"))
        .awaited()
        .expect_err("refused");
    assert_eq!(refused.grpc, GrpcCode::FailedPrecondition);
    assert_eq!(refused.code, "TABLE_FULL");
    assert_eq!(refused.extras.get("seats").map(String::as_str), Some("6"));
    assert_eq!(refused.refusal.as_deref(), Some(&fake_child::refusal()));
    assert!(!is_retryable(&refused));
    host.shutdown();
}

/// The handler start hands back reaches its child the moment start
/// returns: the supervisor, held right after telling start the child is up,
/// has published the child's connection already.
#[test]
fn a_handler_reaches_its_child_as_soon_as_start_returns() {
    let hold = Arc::new(tokio::sync::Notify::new());
    let hooks = Hooks {
        reported_up: Some(hold.clone()),
        ..Hooks::default()
    };
    let host = Host::start_with(config(vec![player(mode::SERVE)]), hooks).expect("the host starts");
    let answered = host
        .handler("player")
        .unwrap()
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited();
    hold.notify_one();
    assert!(answered.is_ok(), "{answered:?}");
    host.shutdown();
}

/// A child that dies is unreachable, which is retryable; it is restarted,
/// as a new process, and the same handler reaches it.
#[test]
fn a_dead_child_is_retryable_then_restarted() {
    let host = started(vec![player(mode::SERVE)]);
    let handler = host.handler("player").unwrap();
    let first = pid_of(&handler);
    let died = handler
        .dispatch(&command("player", "/examples.blackjack.Die"))
        .awaited()
        .expect_err("the child died mid-call");
    assert!(is_retryable(&died), "{died:?}");
    // The call reached the child and the child went away under it: not a
    // call that found no child, which would leave this one alive.
    assert_eq!(
        died.extras.get(crate::remote::CAUSE).map(String::as_str),
        Some(crate::remote::causes::TRANSPORT),
        "{died:?}"
    );

    // The second child coming up is awaited, not polled for: the
    // supervisor counts each child once its client is set. The timeout is
    // a safety net for a broken restart, never the way the test passes.
    let mut generations = handler.generations();
    let restarted = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(20), generations.wait_for(|n| *n >= 2))
                .await
                .map(|seen| seen.is_ok())
        });
    assert_eq!(restarted, Ok(true), "the child was restarted");
    let answered = handler
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .expect("the restarted child answers");
    assert!(answered.rules_version.starts_with("pid-"));
    assert_ne!(answered.rules_version, first, "a new child answered");
    assert!(!alive(&first));
    host.shutdown();
}

/// A child that never answers fails the call retryably once the call
/// timeout passes, rather than blocking the caller; the child is left to
/// its supervisor.
#[test]
fn a_hung_child_times_out_retryably() {
    let mut config = config(vec![player(mode::SERVE)]);
    config.supervision.call_timeout_ms = 300;
    let host = Host::start(config).unwrap();
    let handler = host.handler("player").unwrap();
    let started = Instant::now();
    let hung = handler
        .dispatch(&command("player", "/examples.blackjack.Hang"))
        .awaited()
        .expect_err("timed out");
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(is_retryable(&hung), "{hung:?}");
    assert_eq!(
        hung.extras.get(crate::remote::CAUSE).map(String::as_str),
        Some(crate::remote::causes::TIMEOUT)
    );
    assert!(handler
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .is_ok());
    host.shutdown();
}

/// A restart that fails waits twice as long before the next, up to the
/// configured most.
#[test]
fn the_backoff_doubles_up_to_its_most() {
    let ms = Duration::from_millis;
    assert_eq!(next_backoff(ms(100), ms(5_000)), ms(200));
    assert_eq!(next_backoff(ms(3_000), ms(5_000)), ms(5_000));
    assert_eq!(next_backoff(ms(5_000), ms(5_000)), ms(5_000));
}

/// Stdout ending after the handshake is not the child ending: it keeps
/// serving, the same process. The fake closes stdout before it serves its
/// first call, so the end is already there when the call is made.
#[test]
fn stdout_ending_after_the_handshake_is_not_the_child_ending() {
    let host = started(vec![player(mode::CLOSES_STDOUT)]);
    let handler = host.handler("player").unwrap();
    for _ in 0..3 {
        let response = handler
            .dispatch(&command("player", "/examples.blackjack.SitDown"))
            .awaited()
            .expect("still serving");
        assert_eq!(response.rules_version, pid_of(&handler));
    }
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// The default socket directory is short: `$XDG_RUNTIME_DIR` when it is
/// absolute and fits, else `/tmp`; never a deep `$TMPDIR`.
#[test]
fn the_default_socket_dir_is_short() {
    let xdg = PathBuf::from("/run/user/1000");
    assert_eq!(socket_dir(None, Some(xdg.clone())), Ok(xdg));
    assert_eq!(socket_dir(None, None), Ok(PathBuf::from("/tmp")));
    assert_eq!(
        socket_dir(None, Some(PathBuf::from("run/user/1000"))),
        Ok(PathBuf::from("/tmp"))
    );
    let deep = PathBuf::from(format!("/{}", "d".repeat(80)));
    assert_eq!(socket_dir(None, Some(deep)), Ok(PathBuf::from("/tmp")));
    let path = socket_path(Path::new("/tmp"));
    assert!(path.as_os_str().len() <= MAX_SOCKET_PATH);
}

/// A configured directory is used as given, up to the longest that holds
/// the host's socket names, and refused past it.
#[test]
fn a_configured_socket_dir_too_long_is_refused() {
    let max = MAX_SOCKET_PATH - MAX_SOCKET_NAME;
    let fits = PathBuf::from(format!("/{}", "d".repeat(max - 1)));
    assert_eq!(socket_dir(Some(&fits), None), Ok(fits.clone()));
    let worst = fits.join(format!("angzarr-{}-{}.sock", 4_194_304, u64::MAX));
    assert!(worst.as_os_str().len() <= MAX_SOCKET_PATH);
    let long = PathBuf::from(format!("/{}", "d".repeat(max)));
    assert_eq!(
        socket_dir(Some(&long), None),
        Err(HostError::SocketDirTooLong {
            dir: long.display().to_string(),
            max_bytes: max as u64,
        })
    );
}

/// The refusal comes at start, before any child is launched.
#[test]
fn start_refuses_a_socket_dir_too_long() {
    let mut config = config(vec![player(mode::SERVE)]);
    let long = PathBuf::from(format!("/{}", "d".repeat(100)));
    config.supervision.socket_dir = Some(long.clone());
    assert_eq!(
        Host::start(config).err(),
        Some(HostError::SocketDirTooLong {
            dir: long.display().to_string(),
            max_bytes: (MAX_SOCKET_PATH - MAX_SOCKET_NAME) as u64,
        })
    );
}

/// The real Python player (client-python's harness, examples-python's
/// player bot), launched by its default: `uv run --locked angzarr-player`.
/// Ignored by default: it needs uv, the player's checkout, and its locked
/// dependencies. `ANGZARR_PLAYER_DIR` names the checkout's `player`
/// directory; `~/workspace/worktrees/examples-python--player-bot/player`
/// otherwise.
#[test]
#[ignore = "needs uv and the Python player's checkout"]
fn the_python_player_comes_up_by_its_default_launch() {
    let dir = std::env::var_os("ANGZARR_PLAYER_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap())
                .join("workspace/worktrees/examples-python--player-bot/player")
        });
    let player = ComponentConfig {
        name: "player".to_string(),
        language: "python".to_string(),
        kind: crate::config::ComponentKindConfig::Domain,
        upcasts: None,
        launch: None,
        entry: vec!["angzarr-player".to_string()],
        dir: Some(dir),
        env: Default::default(),
        expose: Default::default(),
    };
    let mut config = config(vec![player]);
    config.supervision.handshake_timeout_ms = 120_000;
    let host = Host::start(config).expect("the player comes up");
    let handler = host.handler("player").unwrap();
    assert_eq!(handler.kind(), pb::ComponentKind::Aggregate);
    assert!(!handler.name().is_empty());
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// Chatter on stdout before the handshake is skipped.
#[test]
fn stdout_before_the_handshake_is_skipped() {
    let host = started(vec![player(mode::NOISY)]);
    assert!(host
        .handler("player")
        .unwrap()
        .dispatch(&command("player", "/examples.blackjack.SitDown"))
        .awaited()
        .is_ok());
    host.shutdown();
}

#[test]
fn a_child_that_never_handshakes_fails_start() {
    let mut config = config(vec![player(mode::SILENT)]);
    config.supervision.handshake_timeout_ms = 500;
    let started = Instant::now();
    assert_eq!(
        Host::start(config).err(),
        Some(HostError::Handshake {
            domain: "player".to_string(),
            cause: HandshakeError::Timeout { timeout_ms: 500 },
        })
    );
    assert!(started.elapsed() >= Duration::from_millis(500));
}

#[test]
fn a_malformed_handshake_fails_start() {
    assert_eq!(
        Host::start(config(vec![player(mode::MALFORMED)])).err(),
        Some(HostError::Handshake {
            domain: "player".to_string(),
            cause: HandshakeError::Malformed {
                line: fake_child::MALFORMED_LINE.to_string(),
            },
        })
    );
}

/// The config's launch is what runs: a program that does not exist fails
/// start by name, where the default (uv) would have been tried otherwise.
#[test]
fn the_configured_launch_is_what_runs() {
    let mut player = player(mode::SERVE);
    player.launch = Some(vec!["/nonexistent/angzarr-player".to_string()]);
    assert_eq!(
        Host::start(config(vec![player])).err(),
        Some(HostError::Spawn {
            domain: "player".to_string(),
            program: "/nonexistent/angzarr-player".to_string(),
            kind: "NotFound".to_string(),
        })
    );
}

/// A handler is named by the component its child describes, and found by
/// the domain it is configured for: the two differ (`PlayerAggregate` in
/// the domain `player`).
#[test]
fn a_handler_is_named_by_its_component_and_found_by_its_domain() {
    let host = started(vec![domain("player", "PlayerAggregate", mode::SERVE)]);
    let handler = host.handler("player").expect("found by its domain");
    assert_eq!(handler.name(), "PlayerAggregate");
    assert_eq!(handler.domain(), "player");
    assert!(host.handler("PlayerAggregate").is_none());
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

#[test]
fn a_domain_configured_twice_is_refused() {
    assert_eq!(
        Host::start(config(vec![player(mode::SERVE), player(mode::SERVE)])).err(),
        Some(HostError::DuplicateDomain {
            domain: "player".to_string(),
        })
    );
}

/// A start that fails leaves no child behind: the domains that came up
/// before the failing one are stopped.
#[test]
fn a_failed_start_stops_the_children_it_started() {
    let tag = format!("failed-start-{}", std::process::id());
    let mut config = config(vec![
        domain("dealer", "dealer", mode::SERVE),
        player(mode::MALFORMED),
    ]);
    for domain in &mut config.components {
        domain.env.insert(TAG_ENV.to_string(), tag.clone());
    }
    assert!(Host::start(config).is_err());
    assert_eq!(
        running_with_tag(&tag),
        0,
        "a child outlived the failed start"
    );
}

/// Shutdown closes the child's stdin, and the child exits by itself.
#[test]
fn shutdown_closes_stdin_and_the_child_exits() {
    let host = started(vec![player(mode::SERVE)]);
    let pid = pid_of(&host.handler("player").unwrap());
    let started = Instant::now();
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
    assert!(
        started.elapsed() < Duration::from_millis(3_000),
        "exited before the grace ran out"
    );
    assert!(!alive(&pid));
}

/// A child that ignores its stdin's end is sent SIGTERM once the grace
/// runs out, and exits on it.
#[test]
fn a_child_ignoring_stdin_eof_is_terminated_after_the_grace() {
    let mut config = config(vec![player(mode::STUBBORN)]);
    config.supervision.shutdown_grace_ms = 300;
    let host = Host::start(config).unwrap();
    let pid = pid_of(&host.handler("player").unwrap());
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Terminated)]
    );
    assert!(!alive(&pid));
}

/// A child that ignores SIGTERM too is killed once its grace runs out.
#[test]
fn a_child_ignoring_sigterm_is_killed_after_its_grace() {
    let mut config = config(vec![player(mode::DEAF)]);
    config.supervision.shutdown_grace_ms = 200;
    config.supervision.terminate_grace_ms = 300;
    let host = Host::start(config).unwrap();
    let pid = pid_of(&host.handler("player").unwrap());
    let started = Instant::now();
    assert_eq!(
        host.shutdown(),
        vec![("player".to_string(), ChildExit::Killed)]
    );
    assert!(started.elapsed() >= Duration::from_millis(500));
    assert!(!alive(&pid));
}

/// Dropping the host stops its children as shutdown does.
#[test]
fn dropping_the_host_stops_its_children() {
    let mark = std::env::temp_dir().join(format!(
        "angzarr-host-drop-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&mark);
    let mut player = player(mode::SERVE);
    player.env.insert(
        fake_child::EOF_MARK_ENV.to_string(),
        mark.display().to_string(),
    );
    let host = started(vec![player]);
    let pid = pid_of(&host.handler("player").unwrap());
    drop(host);
    assert!(!alive(&pid));
    // Stopped by its stdin's end, and waited for: not killed, as a
    // dropped runtime would have it (kill_on_drop).
    assert!(mark.exists(), "the child exited on stdin's end");
    let _ = std::fs::remove_file(&mark);
}

/// A variable the tests set on a child to find it again.
const TAG_ENV: &str = "ANGZARR_FAKE_TAG";

/// How many running (not zombie) processes have `ANGZARR_FAKE_TAG=tag` in
/// their environment.
fn running_with_tag(tag: &str) -> usize {
    let wanted = format!("{TAG_ENV}={tag}");
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let Some(pid) = e.file_name().to_str().map(str::to_string) else {
                return false;
            };
            pid.bytes().all(|b| b.is_ascii_digit())
                && alive(&format!("pid-{pid}"))
                && std::fs::read(e.path().join("environ"))
                    .is_ok_and(|env| env.split(|&b| b == 0).any(|var| var == wanted.as_bytes()))
        })
        .count()
}

/// A valid descriptor of `kind` named "order": what angzarr-cli generates.
fn described(kind: pb::ComponentKind) -> pb::ComponentDescriptor {
    let stateful = matches!(
        kind,
        pb::ComponentKind::Aggregate | pb::ComponentKind::ProcessManager
    );
    let policy = kind == pb::ComponentKind::Policy;
    pb::ComponentDescriptor {
        name: "order".to_string(),
        kind: kind as i32,
        rules_version: "1.4.2+build.7".to_string(),
        state: stateful.then(|| pb::StateDescriptor {
            type_url: "/orders.v1.OrderState".to_string(),
            options: None,
        }),
        inputs: policy
            .then(|| pb::Target {
                domain: "order".to_string(),
            })
            .into_iter()
            .collect(),
        output_domain: if policy {
            "inventory".to_string()
        } else {
            String::new()
        },
        starting_types: if kind == pb::ComponentKind::ProcessManager {
            vec!["orders.v1.Place".to_string()]
        } else {
            Vec::new()
        },
    }
}

/// C-0687: the host refuses a policy's descriptor with no output domain,
/// naming the component, under COORDINATOR_REASON_POLICY_NO_OUTPUT_DOMAIN.
#[test]
fn a_policy_descriptor_without_an_output_domain_is_refused() {
    use angzarr_diagnostic::Diagnostic;
    let mut descriptor = described(pb::ComponentKind::Policy);
    assert_eq!(crate::validate(&descriptor), Ok(()));
    descriptor.output_domain = String::new();
    let refused = crate::validate(&descriptor).err().unwrap();
    assert_eq!(
        refused.reason(),
        "COORDINATOR_REASON_POLICY_NO_OUTPUT_DOMAIN"
    );
    assert_eq!(
        refused,
        crate::DescriptorRefused::PolicyNoOutputDomain {
            component: "order".to_string()
        }
    );
    assert_eq!(
        refused.code(),
        "angzarr-embedded-host-descriptor-policy-no-output-domain"
    );
}

/// C-0687, every row: each fault refused under its own reason, a valid
/// descriptor of every kind accepted.
#[test]
fn a_descriptor_that_fails_its_validation_is_refused_for_its_reason() {
    use pb::ComponentKind::{Aggregate, Policy, ProcessManager, Projector};
    for kind in [Aggregate, Policy, ProcessManager, Projector] {
        assert_eq!(crate::validate(&described(kind)), Ok(()), "{kind:?}");
    }
    let reason = |kind, fault: Fault| {
        let mut descriptor = described(kind);
        fault(&mut descriptor);
        crate::validate(&descriptor).err().map(|r| r.reason())
    };
    type Fault = fn(&mut pb::ComponentDescriptor);
    let cases: [(pb::ComponentKind, Fault, &str); 9] = [
        (
            Aggregate,
            |d| d.rules_version.clear(),
            "COORDINATOR_REASON_MISSING_RULES_VERSION",
        ),
        (
            Policy,
            |d| d.rules_version.clear(),
            "COORDINATOR_REASON_MISSING_RULES_VERSION",
        ),
        (
            Aggregate,
            |d| d.state = None,
            "COORDINATOR_REASON_MISSING_STATE_TYPE",
        ),
        (
            ProcessManager,
            |d| d.state = None,
            "COORDINATOR_REASON_MISSING_STATE_TYPE",
        ),
        (
            Projector,
            |d| d.state = Some(Default::default()),
            "COORDINATOR_REASON_STATE_ON_STATELESS_KIND",
        ),
        (
            Aggregate,
            |d| d.starting_types = vec!["orders.v1.Place".to_string()],
            "COORDINATOR_REASON_STARTING_TYPES_ON_NON_PM",
        ),
        (
            Policy,
            |d| d.output_domain.clear(),
            "COORDINATOR_REASON_POLICY_NO_OUTPUT_DOMAIN",
        ),
        (
            Policy,
            |d| {
                d.inputs.push(pb::Target {
                    domain: "payment".to_string(),
                })
            },
            "COORDINATOR_REASON_POLICY_INPUT_COUNT",
        ),
        (
            ProcessManager,
            |d| d.output_domain = "inventory".to_string(),
            "COORDINATOR_REASON_OUTPUT_DOMAIN_ON_NON_POLICY",
        ),
    ];
    for (kind, fault, expected) in cases {
        assert_eq!(reason(kind, fault), Some(expected), "{kind:?}");
    }
    let mut none = described(Policy);
    none.inputs.clear();
    assert_eq!(
        crate::validate(&none),
        Err(crate::DescriptorRefused::PolicyInputCount {
            component: "order".to_string(),
            input_count: 0
        })
    );
}

/// A host dropped inside a current-thread runtime stops its children
/// there, as it does anywhere: that runtime has no worker to hand off, so
/// nothing is asked of it.
#[test]
fn a_host_dropped_on_a_current_thread_runtime_stops_its_children() {
    let host = started(vec![player(mode::SERVE)]);
    let pid = pid_of(&host.handler("player").unwrap());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { drop(host) });
    assert!(!alive(&pid));
}

/// A restarted child is registered by its new descriptor only if it is
/// the same component: its name and kind, and for a policy or projector
/// its inputs and output domain, which its registration routes by. A
/// domain may change anything else (its rules version, its state, its
/// starting types).
#[test]
fn a_restarted_child_must_describe_the_same_component() {
    use pb::ComponentKind::{Aggregate, Policy, Projector};
    use ComponentKindConfig::{Domain, Policy as PolicyKind, Projector as ProjectorKind};
    let aggregate = described(Aggregate);
    let rebuilt = pb::ComponentDescriptor {
        rules_version: "1.5.0".to_string(),
        state: None,
        inputs: vec![pb::Target {
            domain: "elsewhere".to_string(),
        }],
        ..aggregate.clone()
    };
    assert!(same_component(Domain, &aggregate, &rebuilt));
    let renamed = pb::ComponentDescriptor {
        name: "other".to_string(),
        ..aggregate.clone()
    };
    assert!(!same_component(Domain, &aggregate, &renamed));
    let rekinded = pb::ComponentDescriptor {
        kind: pb::ComponentKind::ProcessManager as i32,
        ..aggregate.clone()
    };
    assert!(!same_component(Domain, &aggregate, &rekinded));

    let policy = described(Policy);
    let rebuilt = pb::ComponentDescriptor {
        rules_version: "1.5.0".to_string(),
        ..policy.clone()
    };
    assert!(same_component(PolicyKind, &policy, &rebuilt));
    let rerouted = pb::ComponentDescriptor {
        output_domain: "billing".to_string(),
        ..policy.clone()
    };
    assert!(!same_component(PolicyKind, &policy, &rerouted));
    let resourced = pb::ComponentDescriptor {
        inputs: vec![pb::Target {
            domain: "cart".to_string(),
        }],
        ..policy.clone()
    };
    assert!(!same_component(PolicyKind, &policy, &resourced));

    let projector = pb::ComponentDescriptor {
        inputs: vec![pb::Target {
            domain: "order".to_string(),
        }],
        ..described(Projector)
    };
    let more = pb::ComponentDescriptor {
        inputs: vec![
            pb::Target {
                domain: "order".to_string(),
            },
            pb::Target {
                domain: "cart".to_string(),
            },
        ],
        ..projector.clone()
    };
    assert!(same_component(
        ProjectorKind,
        &projector,
        &projector.clone()
    ));
    assert!(!same_component(ProjectorKind, &projector, &more));
}

/// A restarted child describing another component is not registered, and
/// leaves its component's registration as it was: the host refuses it
/// ([`HostError::DescriptorChanged`]); one the registry refuses is
/// [`HostError::Register`]. The first child's is taken as it is.
#[test]
fn a_child_described_again_is_refused_unless_the_registry_takes_it() {
    let supervisor = |kind| Supervisor {
        domain: "order".to_string(),
        kind,
        launch: Launch {
            program: "true".to_string(),
            args: Vec::new(),
            env: Default::default(),
            dir: None,
        },
        supervision: Supervision::default(),
        socket_dir: PathBuf::from("/tmp"),
        link: Arc::new(crate::remote::Link::default()),
        hooks: Hooks::default(),
    };
    let order = supervisor(ComponentKindConfig::Domain);
    let first = pb::ComponentDescriptor {
        state: Some(pb::StateDescriptor {
            type_url: "/orders.v1.OrderState".to_string(),
            options: Some(pb::StateOptions {
                conflict_resolutions: vec![
                    pb::ConflictResolution::Strict as i32,
                    pb::ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }),
        ..described(pb::ComponentKind::Aggregate)
    };
    assert_eq!(order.registered_by(&first, true), Ok(()));
    assert_eq!(order.link.registered().descriptor, first);
    let renamed = pb::ComponentDescriptor {
        name: "other".to_string(),
        ..first.clone()
    };
    assert_eq!(
        order.registered_by(&renamed, false),
        Err(HostError::DescriptorChanged {
            domain: "order".to_string()
        })
    );
    assert_eq!(order.link.registered().descriptor, first, "the old kept");

    let mut registry = Registry::new();
    let description = registry
        .described_handler(
            "order",
            RemoteCommandHandler::new(
                "order".to_string(),
                first.clone(),
                order.link.clone(),
                Duration::from_millis(1),
            ),
            first.clone(),
        )
        .unwrap();
    order.link.registered().description = Some(description);
    let unversioned = pb::ComponentDescriptor {
        rules_version: String::new(),
        ..first.clone()
    };
    let refused = order.registered_by(&unversioned, false);
    assert!(
        matches!(&refused, Err(HostError::Register { cause }) if angzarr_diagnostic::Diagnostic::code(cause) == "angzarr-embedded-misconfigured-empty-rules-version"),
        "{refused:?}"
    );
    assert_eq!(order.link.registered().descriptor, first, "the old kept");
    let rebuilt = pb::ComponentDescriptor {
        rules_version: "1.5.0".to_string(),
        ..first.clone()
    };
    assert_eq!(order.registered_by(&rebuilt, false), Ok(()));
    assert_eq!(order.link.registered().descriptor, rebuilt);
    assert_eq!(registry.rules_version("order").as_deref(), Some("1.5.0"));

    let sidecar = supervisor(ComponentKindConfig::Upcaster);
    assert_eq!(sidecar.registered_by(&renamed, false), Ok(()));
    assert_eq!(
        sidecar.link.registered().descriptor,
        pb::ComponentDescriptor::default(),
        "a sidecar is not described"
    );
}

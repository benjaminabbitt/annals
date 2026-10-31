//! The host's errors: why a remote component could not be started. Each is a
//! code and its args (`angzarr_diagnostic::Diagnostic`), and its
//! `Display` is those, never English. A failure once a domain is running is not
//! one of these: it is the router's [`CodedError`], from
//! [`RemoteCommandHandler`]'s dispatch.
//!
//! [`CodedError`]: angzarr_router::error::CodedError
//! [`RemoteCommandHandler`]: crate::RemoteCommandHandler

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, Report};

/// What was wrong with a child's handshake: the one stdout line,
/// `angzarr-handshake v1 unix:<absolute-socket-path>`, it prints once it is
/// serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeError {
    /// A line that announces itself as a handshake (it starts
    /// `angzarr-handshake`) but is not a v1 one naming an absolute Unix
    /// socket path. `line` is the line as read, its line ending removed.
    Malformed { line: String },
    /// No handshake within the configured time.
    Timeout { timeout_ms: u64 },
    /// The child's stdout ended (it exited, or closed it) before a
    /// handshake.
    Closed,
    /// Reading the child's stdout failed. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    Unreadable { kind: String },
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for HandshakeError {
    fn code(&self) -> &str {
        match self {
            HandshakeError::Malformed { .. } => "angzarr-embedded-host-handshake-malformed",
            HandshakeError::Timeout { .. } => "angzarr-embedded-host-handshake-timeout",
            HandshakeError::Closed => "angzarr-embedded-host-handshake-closed",
            HandshakeError::Unreadable { .. } => "angzarr-embedded-host-handshake-unreadable",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            HandshakeError::Malformed { line } => vec![("line", line.as_str().into())],
            HandshakeError::Timeout { timeout_ms } => vec![("timeout_ms", (*timeout_ms).into())],
            HandshakeError::Closed => vec![],
            HandshakeError::Unreadable { kind } => vec![("kind", kind.as_str().into())],
        }
    }
}

/// Why the host could not start: a config it refuses, or a remote
/// component that did not come up. Every variant about one component names
/// it as `domain`: its `[[component]]` name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostError {
    /// Two `[[component]]` tables with one name.
    DuplicateDomain { domain: String },
    /// A remote component whose language has no default launch command, and
    /// whose config gives none.
    NoLaunch { domain: String, language: String },
    /// A `launch` with no program in it.
    EmptyLaunch { domain: String },
    /// A component whose `language` is not one of the languages loaded
    /// (`config::Languages`): `key` its TOML key path
    /// (`component[1].language`), `loaded` the loaded languages' names, in
    /// order, comma-separated.
    UnknownLanguage {
        key: String,
        language: String,
        loaded: String,
    },
    /// The launch command's program could not be started. `kind` is the I/O
    /// error's `std::io::ErrorKind`, by name.
    Spawn {
        domain: String,
        program: String,
        kind: String,
    },
    /// The child started but its handshake failed.
    Handshake {
        domain: String,
        cause: HandshakeError,
    },
    /// No gRPC connection to the socket the handshake named.
    Connect { domain: String, socket: String },
    /// `ComponentService.Describe` failed; `grpc_code` is the status's wire
    /// code.
    Describe { domain: String, grpc_code: i32 },
    /// An upcaster sidecar's `grpc.health.v1.Health.Watch` failed (decision
    /// 14.39: a sidecar serves gRPC health); `grpc_code` is the status's
    /// wire code.
    Health { domain: String, grpc_code: i32 },
    /// The child described itself as another kind than its `kind` says:
    /// `described` is its descriptor's `ComponentKind`, by name.
    KindMismatch {
        domain: String,
        configured: String,
        described: String,
    },
    /// A policy or projector whose descriptor names no input domain: it
    /// has no source.
    NoInput { domain: String },
    /// A `kind = "upcaster"` component that names no domain to upcast
    /// (`upcasts`).
    UpcasterNoDomain { domain: String },
    /// A component given an `expose` other than `"none"` that is not an
    /// aggregate: `kind` is its `kind`, by its TOML name. Only an aggregate
    /// is served through the host's listener.
    NotExposable { domain: String, kind: String },
    /// The child's descriptor fails the coordinator's validation
    /// (decision 14.42, C-0687; [`DescriptorRefused`](crate::DescriptorRefused)).
    Descriptor {
        domain: String,
        cause: crate::DescriptorRefused,
    },
    /// A restarted child described another component than its first
    /// child did: another name or kind, or a policy or projector of other
    /// inputs or another output domain. It is not published; the host
    /// takes such a change only when it starts.
    DescriptorChanged { domain: String },
    /// The configured `socket_dir` is too long for a Unix socket path to
    /// hold the host's socket names in it: at most `max_bytes` bytes.
    SocketDirTooLong { dir: String, max_bytes: u64 },
    /// The config file could not be read. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    ConfigUnreadable { path: String, kind: String },
    /// The config file is not a valid config: malformed TOML, an unknown
    /// key, a missing one, a value outside its closed set or of the wrong
    /// type. `key` is the TOML key path where it goes wrong
    /// (`component[1].kind`; a missing key's table; empty for TOML that
    /// does not parse), `offset` the byte where the problem starts, and
    /// `ext_err_txt` the TOML reader's text, as it said it.
    ConfigInvalid {
        path: String,
        key: String,
        offset: u64,
        ext_err_txt: String,
    },
    /// An in-process component its `[[component]]` lists (`kind` its TOML
    /// kind) that the application's `register` did not register: no
    /// domain, policy or projector of that name, or, for an upcaster, no
    /// domain here of its `upcasts`.
    NotRegistered { domain: String, kind: String },
    /// A component the application's `register` registered in process
    /// (`kind` its TOML kind) that no `[[component]]` lists as an
    /// in-process component of that kind: every component is listed.
    NotListed { domain: String, kind: String },
    /// The registry refused a domain (the kernel's `Misconfigured`).
    Register { cause: Report },
    /// The store could not be opened (the kernel's store error).
    Store { cause: Report },
    /// The host's own runtime could not be built. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    Runtime { kind: String },
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for HostError {
    fn code(&self) -> &str {
        match self {
            HostError::DuplicateDomain { .. } => "angzarr-embedded-host-duplicate-domain",
            HostError::NoLaunch { .. } => "angzarr-embedded-host-no-launch",
            HostError::EmptyLaunch { .. } => "angzarr-embedded-host-empty-launch",
            HostError::UnknownLanguage { .. } => "angzarr-embedded-host-unknown-language",
            HostError::Spawn { .. } => "angzarr-embedded-host-spawn",
            HostError::Handshake { .. } => "angzarr-embedded-host-handshake",
            HostError::Connect { .. } => "angzarr-embedded-host-connect",
            HostError::Describe { .. } => "angzarr-embedded-host-describe",
            HostError::Health { .. } => "angzarr-embedded-host-health",
            HostError::KindMismatch { .. } => "angzarr-embedded-host-kind-mismatch",
            HostError::NoInput { .. } => "angzarr-embedded-host-no-input",
            HostError::Descriptor { .. } => "angzarr-embedded-host-descriptor",
            HostError::DescriptorChanged { .. } => "angzarr-embedded-host-descriptor-changed",
            HostError::UpcasterNoDomain { .. } => "angzarr-embedded-host-upcaster-no-domain",
            HostError::NotExposable { .. } => "angzarr-embedded-host-not-exposable",
            HostError::SocketDirTooLong { .. } => "angzarr-embedded-host-socket-dir-too-long",
            HostError::ConfigUnreadable { .. } => "angzarr-embedded-host-config-unreadable",
            HostError::ConfigInvalid { .. } => "angzarr-embedded-host-config-invalid",
            HostError::NotRegistered { .. } => "angzarr-embedded-host-not-registered",
            HostError::NotListed { .. } => "angzarr-embedded-host-not-listed",
            HostError::Register { .. } => "angzarr-embedded-host-register",
            HostError::Store { .. } => "angzarr-embedded-host-store",
            HostError::Runtime { .. } => "angzarr-embedded-host-runtime",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let domain = |d: &str| ("domain", Arg::from(d));
        match self {
            HostError::DuplicateDomain { domain: d }
            | HostError::EmptyLaunch { domain: d }
            | HostError::NoInput { domain: d }
            | HostError::DescriptorChanged { domain: d }
            | HostError::UpcasterNoDomain { domain: d } => {
                vec![domain(d)]
            }
            HostError::NoLaunch {
                domain: d,
                language,
            } => vec![domain(d), ("language", language.as_str().into())],
            HostError::Spawn {
                domain: d,
                program,
                kind,
            } => vec![
                domain(d),
                ("program", program.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::Handshake { domain: d, cause } => {
                vec![domain(d), ("cause", Arg::nested(cause))]
            }
            HostError::NotExposable { domain: d, kind }
            | HostError::NotRegistered { domain: d, kind }
            | HostError::NotListed { domain: d, kind } => {
                vec![domain(d), ("kind", kind.as_str().into())]
            }
            HostError::Connect { domain: d, socket } => {
                vec![domain(d), ("socket", socket.as_str().into())]
            }
            HostError::Describe {
                domain: d,
                grpc_code,
            }
            | HostError::Health {
                domain: d,
                grpc_code,
            } => vec![domain(d), ("grpc_code", (*grpc_code).into())],
            HostError::KindMismatch {
                domain: d,
                configured,
                described,
            } => vec![
                domain(d),
                ("configured", configured.as_str().into()),
                ("described", described.as_str().into()),
            ],
            HostError::Descriptor { domain: d, cause } => {
                vec![domain(d), ("cause", Arg::nested(cause))]
            }
            HostError::SocketDirTooLong { dir, max_bytes } => vec![
                ("dir", dir.as_str().into()),
                ("max_bytes", (*max_bytes).into()),
            ],
            HostError::UnknownLanguage {
                key,
                language,
                loaded,
            } => vec![
                ("key", key.as_str().into()),
                ("language", language.as_str().into()),
                ("loaded", loaded.as_str().into()),
            ],
            HostError::ConfigUnreadable { path, kind } => vec![
                ("path", path.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::ConfigInvalid {
                path,
                key,
                offset,
                ext_err_txt,
            } => vec![
                ("path", path.as_str().into()),
                ("key", key.as_str().into()),
                ("offset", (*offset).into()),
                ("ext_err_txt", ext_err_txt.as_str().into()),
            ],
            HostError::Register { cause } | HostError::Store { cause } => {
                vec![("cause", Arg::nested(cause))]
            }
            HostError::Runtime { kind } => vec![("kind", kind.as_str().into())],
        }
    }
}

impl std::error::Error for HandshakeError {}
impl std::error::Error for HostError {}

/// One value of every error this module defines, each variant once.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let d = || "player".to_string();
    vec![
        Box::new(HandshakeError::Malformed {
            line: "angzarr-handshake v2".to_string(),
        }),
        Box::new(HandshakeError::Timeout { timeout_ms: 30_000 }),
        Box::new(HandshakeError::Closed),
        Box::new(HandshakeError::Unreadable {
            kind: "BrokenPipe".to_string(),
        }),
        Box::new(HostError::DuplicateDomain { domain: d() }),
        Box::new(HostError::NoLaunch {
            domain: d(),
            language: "go".to_string(),
        }),
        Box::new(HostError::EmptyLaunch { domain: d() }),
        Box::new(HostError::UnknownLanguage {
            key: "component[1].language".to_string(),
            language: "go".to_string(),
            loaded: "python,rust".to_string(),
        }),
        Box::new(HostError::Spawn {
            domain: d(),
            program: "uv".to_string(),
            kind: "NotFound".to_string(),
        }),
        Box::new(HostError::Handshake {
            domain: d(),
            cause: HandshakeError::Closed,
        }),
        Box::new(HostError::Connect {
            domain: d(),
            socket: "/run/angzarr/player.sock".to_string(),
        }),
        Box::new(HostError::Describe {
            domain: d(),
            grpc_code: 14,
        }),
        Box::new(HostError::Health {
            domain: d(),
            grpc_code: 12,
        }),
        Box::new(HostError::KindMismatch {
            domain: d(),
            configured: "policy".to_string(),
            described: "AGGREGATE".to_string(),
        }),
        Box::new(HostError::NoInput { domain: d() }),
        Box::new(HostError::UpcasterNoDomain { domain: d() }),
        Box::new(HostError::NotExposable {
            domain: d(),
            kind: "policy".to_string(),
        }),
        Box::new(HostError::Descriptor {
            domain: d(),
            cause: crate::DescriptorRefused::PolicyNoOutputDomain {
                component: "TableWalletPolicy".to_string(),
            },
        }),
        Box::new(HostError::DescriptorChanged { domain: d() }),
        Box::new(HostError::SocketDirTooLong {
            dir: "/very/deep/directory".to_string(),
            max_bytes: 65,
        }),
        Box::new(HostError::ConfigUnreadable {
            path: "angzarr.toml".to_string(),
            kind: "NotFound".to_string(),
        }),
        Box::new(HostError::ConfigInvalid {
            path: "angzarr.toml".to_string(),
            key: "component[1].kind".to_string(),
            offset: 12,
            ext_err_txt: "unknown variant `saga`".to_string(),
        }),
        Box::new(HostError::NotRegistered {
            domain: d(),
            kind: "policy".to_string(),
        }),
        Box::new(HostError::NotListed {
            domain: d(),
            kind: "projector".to_string(),
        }),
        Box::new(HostError::Register {
            cause: Report::new("angzarr-embedded-host-duplicate-domain").arg("domain", "player"),
        }),
        Box::new(HostError::Store {
            cause: Report::new("angzarr-embedded-host-handshake-closed"),
        }),
        Box::new(HostError::Runtime {
            kind: "Other".to_string(),
        }),
    ]
}

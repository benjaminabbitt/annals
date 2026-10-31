//! The host: launches every remote component of a [`HostConfig`], waits for
//! its handshake, connects, describes it, and hands back a handler of its
//! kind (a [`RemoteCommandHandler`] per domain, a [`RemotePolicyHandler`]
//! per policy, a [`RemoteProjectorHandler`] per projector) to register with
//! the application's registry beside its in-process ones. Then it
//! supervises: a child that
//! exits is restarted, after a backoff that grows while restarts fail; while
//! it is down its handler's dispatch is retryable
//! ([`crate::is_retryable`]). A restarted child is described again, and its
//! component registered by its new descriptor before it is published
//! (C-0563): a registered domain's whole descriptor is replaced, and a
//! child describing another component (another name or kind, or a policy
//! or projector of other inputs or another output domain) is not published. An upcaster sidecar is not described: it is
//! up while its gRPC health reports SERVING, and down while it does not.
//!
//! The generated `main.rs` calls [`crate::run`], which holds the
//! in-process registrations to the config's listing; by hand, in
//! outline:
//!
//! ```ignore
//! let config = config::parse(include_str!("angzarr.yaml"), "angzarr.yaml")?;
//! let host = Host::start(config)?;
//! let mut registry = Registry::new();
//! registry.aggregate::<table::Table>(Default::default)?; // in-process
//! host.register(&mut registry)?; // every remote component, by its kind
//! let kernel = Kernel::new(&registry, &dead_letters);
//! // ... kernel.handle_any(&store, book, &sampled, &mut ()) for any domain;
//! // dropping `host` stops the children.
//! ```
//!
//! The host supervises its children on a tokio runtime of its own, on its
//! own threads, so starting and stopping it works the same under any
//! caller; an async caller does both off its workers
//! (`tokio::task::spawn_blocking`), as [`crate::run`] does. A remote
//! component's calls are not the host's: each is awaited on the caller's
//! runtime, which must be tokio's, with its timer.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use angzarr_embedded::{Misconfigured, Registry};
use angzarr_router::pb;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::oneshot;

use crate::config::{
    ComponentKindConfig, Expose, HostConfig, Languages, Launch, Placement, Supervision,
};
use crate::error::HostError;
use crate::handshake::{read_handshake, SOCKET_ENV};
use crate::remote::{
    Link, RemoteCommandHandler, RemotePolicyHandler, RemoteProjectorHandler, RemoteUpcaster,
};
use crate::rpc::component::component_service_client::ComponentServiceClient;
use crate::rpc::connect_unix;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// How a child ended at shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildExit {
    /// It exited by itself once its stdin was closed: its exit code, `None`
    /// when a signal ended it.
    Exited(Option<i32>),
    /// It outlived its stdin's grace, and exited once sent SIGTERM.
    Terminated,
    /// It outlived SIGTERM's grace too, and was killed (SIGKILL).
    Killed,
    /// No child was running (it had exited and was waiting to be
    /// restarted).
    NotRunning,
}

/// The running host: its children and their handlers. Dropping it shuts
/// them down as [`Host::shutdown`] does.
pub struct Host {
    runtime: Option<tokio::runtime::Runtime>,
    domains: Vec<Supervised>,
}

/// What a remote component is handed back as, by its kind.
#[derive(Clone)]
enum Hosted {
    Domain(RemoteCommandHandler),
    Policy(RemotePolicyHandler),
    Projector(RemoteProjectorHandler),
    Upcaster(RemoteUpcaster),
}

/// One remote component under supervision.
struct Supervised {
    /// Its `component` entry name.
    name: String,
    hosted: Hosted,
    /// Tells the supervisor to stop the child; taken when sent.
    stop: Option<oneshot::Sender<()>>,
    /// How the child ended, once the supervisor has stopped it. In a
    /// `Mutex` only so the host is `Sync`: it is read once, at shutdown.
    ended: Mutex<mpsc::Receiver<ChildExit>>,
}

impl Host {
    /// Starts every remote component of `config` and waits for each to come
    /// up: launched, handshaken, connected, and described, its descriptor of
    /// the kind its `kind` says ([`HostError::KindMismatch`]; a policy's or
    /// projector's naming its input, [`HostError::NoInput`]; one that
    /// fails validation, [`HostError::Descriptor`]). An upcaster sidecar is
    /// not described: once connected, the host watches its gRPC health
    /// (refused, [`HostError::Health`], when it serves none), and it is up
    /// while that reports SERVING; until then it is handed back down, its
    /// upcasts retryable. One naming no domain to upcast is refused before
    /// any child starts ([`HostError::UpcasterNoDomain`]), as is a
    /// component other than a domain given an `expose`
    /// ([`HostError::NotExposable`]). In-process domains are left to the
    /// caller. Refused, with every child already started stopped again,
    /// when any domain's config is refused or any child fails to come up
    /// the first time. It blocks until then: an async caller calls it off
    /// its workers.
    pub fn start(config: HostConfig) -> Result<Host, HostError> {
        Host::start_with_languages(config, &Languages::builtin())
    }

    /// [`Host::start`] knowing `languages`: the built-in ones and those
    /// the application loads. A component in a language not among them is
    /// refused before any child starts ([`HostError::UnknownLanguage`]).
    pub fn start_with_languages(
        config: HostConfig,
        languages: &Languages,
    ) -> Result<Host, HostError> {
        Host::started(config, languages, Hooks::default())
    }

    /// [`Host::start`], its supervisors held at `hooks`' points.
    #[cfg(test)]
    fn start_with(config: HostConfig, hooks: Hooks) -> Result<Host, HostError> {
        Host::started(config, &Languages::builtin(), hooks)
    }

    /// [`Host::start_with_languages`], its supervisors held at `hooks`'
    /// points.
    fn started(config: HostConfig, languages: &Languages, hooks: Hooks) -> Result<Host, HostError> {
        languages.check(&config)?;
        let mut names = BTreeSet::new();
        let mut remote = Vec::new();
        for (index, component) in config.components.iter().enumerate() {
            if !names.insert(component.name.as_str()) {
                return Err(HostError::DuplicateDomain {
                    domain: component.name.clone(),
                });
            }
            if component.expose != Expose::None && component.kind != ComponentKindConfig::Domain {
                return Err(HostError::NotExposable {
                    domain: component.name.clone(),
                    kind: component.kind.as_str().to_string(),
                });
            }
            let upcasts = component.upcasts.clone().unwrap_or_default();
            if component.kind == ComponentKindConfig::Upcaster && upcasts.is_empty() {
                return Err(HostError::UpcasterNoDomain {
                    domain: component.name.clone(),
                });
            }
            let language = languages.of(&config, index)?;
            if let Placement::Remote(launch) = language.placement(component)? {
                remote.push((component.name.clone(), component.kind, launch, upcasts));
            }
        }
        let socket_dir = socket_dir(
            config.supervision.socket_dir.as_deref(),
            std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        )?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("angzarr-host")
            .enable_all()
            .build()
            .map_err(|e| HostError::Runtime {
                kind: format!("{:?}", e.kind()),
            })?;
        let mut host = Host {
            runtime: None,
            domains: Vec::new(),
        };
        for (domain, kind, launch, upcasts) in remote {
            let link = Arc::new(Link::default());
            let (stop, stopped) = oneshot::channel();
            let (up, came_up) = mpsc::channel();
            let (end, ended) = mpsc::channel();
            let supervisor = Supervisor {
                domain: domain.clone(),
                kind,
                launch,
                supervision: config.supervision.clone(),
                socket_dir: socket_dir.clone(),
                link: link.clone(),
                hooks: hooks.clone(),
            };
            runtime.spawn(async move {
                let _ = end.send(supervisor.run(stopped, up).await);
            });
            let error = match came_up.recv() {
                Ok(Ok(descriptor)) => {
                    let parts = (
                        domain.clone(),
                        descriptor,
                        link,
                        Duration::from_millis(config.supervision.call_timeout_ms),
                    );
                    let hosted = match kind {
                        ComponentKindConfig::Policy => Hosted::Policy(RemotePolicyHandler::new(
                            parts.0, parts.1, parts.2, parts.3,
                        )),
                        ComponentKindConfig::Projector => Hosted::Projector(
                            RemoteProjectorHandler::new(parts.0, parts.1, parts.2, parts.3),
                        ),
                        ComponentKindConfig::Upcaster => Hosted::Upcaster(RemoteUpcaster::new(
                            parts.0, upcasts, parts.2, parts.3,
                        )),
                        _ => Hosted::Domain(RemoteCommandHandler::new(
                            parts.0, parts.1, parts.2, parts.3,
                        )),
                    };
                    host.domains.push(Supervised {
                        name: domain,
                        hosted,
                        stop: Some(stop),
                        ended: Mutex::new(ended),
                    });
                    continue;
                }
                Ok(Err(error)) => error,
                // The supervisor ended without a word: only the runtime
                // going away does that.
                Err(_) => HostError::Runtime {
                    kind: "Interrupted".to_string(),
                },
            };
            // Its supervisor has stopped its child already; stop the rest.
            let _ = ended.recv();
            host.runtime = Some(runtime);
            host.stop_all();
            return Err(error);
        }
        host.runtime = Some(runtime);
        Ok(host)
    }

    /// One handler per remote domain, in the config's order: not its
    /// policies or projectors.
    pub fn handlers(&self) -> Vec<RemoteCommandHandler> {
        self.domains
            .iter()
            .filter_map(|d| match &d.hosted {
                Hosted::Domain(handler) => Some(handler.clone()),
                _ => None,
            })
            .collect()
    }

    /// The handler of the remote domain named `domain`; `None` for a
    /// policy or a projector.
    pub fn handler(&self, domain: &str) -> Option<RemoteCommandHandler> {
        self.hosted(domain).and_then(|hosted| match hosted {
            Hosted::Domain(handler) => Some(handler.clone()),
            _ => None,
        })
    }

    /// The handler of the remote policy configured as `name` (a
    /// `component` entry of `kind: policy`).
    pub fn policy(&self, name: &str) -> Option<RemotePolicyHandler> {
        self.hosted(name).and_then(|hosted| match hosted {
            Hosted::Policy(handler) => Some(handler.clone()),
            _ => None,
        })
    }

    /// The handler of the remote projector configured as `name` (a
    /// `component` entry of `kind: projector`).
    pub fn projector(&self, name: &str) -> Option<RemoteProjectorHandler> {
        self.hosted(name).and_then(|hosted| match hosted {
            Hosted::Projector(handler) => Some(handler.clone()),
            _ => None,
        })
    }

    /// The remote upcaster sidecar configured as `name` (a `component` entry
    /// of `kind = "upcaster"`).
    pub fn upcaster(&self, name: &str) -> Option<RemoteUpcaster> {
        self.hosted(name).and_then(|hosted| match hosted {
            Hosted::Upcaster(upcaster) => Some(upcaster.clone()),
            _ => None,
        })
    }

    fn hosted(&self, name: &str) -> Option<&Hosted> {
        self.domains
            .iter()
            .find(|d| d.name == name)
            .map(|d| &d.hosted)
    }

    /// Registers every remote component into `registry`: a domain's
    /// handler as the domain it is configured for (the kernel then
    /// dispatches that domain's commands, and its policies' steps into it,
    /// through the child: `Kernel::handle_any`); a policy beside every
    /// other policy of its source; a projector of its sources; and, last, an
    /// upcaster sidecar as the port its domain's inputs are upcast through
    /// (decision 14.39), which domain the caller registers first if it is
    /// in-process (one not registered PANICS). In-process
    /// components are registered by the caller as ever. Refused as the
    /// registry refuses a domain (one registered already, a descriptor with
    /// no state or rules version); a policy or projector of a name
    /// registered already PANICS, as the registry's every such
    /// registration does.
    pub fn register(&self, registry: &mut Registry) -> Result<(), Misconfigured> {
        for component in &self.domains {
            match &component.hosted {
                Hosted::Domain(handler) => {
                    // Under the lock the supervisor describes a restarted
                    // child under: registered by the current child's
                    // descriptor, and every later one's replaces it.
                    let mut registered = handler.link().registered();
                    let description = registry.described_handler(
                        handler.domain(),
                        handler.clone(),
                        registered.descriptor.clone(),
                    )?;
                    registered.description = Some(description);
                }
                Hosted::Policy(handler) => {
                    registry.policy_component(handler.clone(), &handler.descriptor().output_domain);
                }
                Hosted::Projector(handler) => {
                    registry.projector_handler(&handler.sources(), handler.clone());
                }
                Hosted::Upcaster(_) => {}
            }
        }
        // Each sidecar upcasts a domain registered by now: a remote one,
        // above, or an in-process one, by the caller before.
        for component in &self.domains {
            if let Hosted::Upcaster(upcaster) = &component.hosted {
                registry.upcast_with(upcaster.upcasts(), upcaster.clone());
            }
        }
        Ok(())
    }

    /// Stops supervising and stops every child: closes its stdin, waits up
    /// to the shutdown grace for it to exit, and kills it if it has not.
    /// How each ended, by domain, in the config's order.
    pub fn shutdown(mut self) -> Vec<(String, ChildExit)> {
        self.stop_all()
    }

    /// Stops every child, all at once, and then the runtime.
    fn stop_all(&mut self) -> Vec<(String, ChildExit)> {
        for domain in &mut self.domains {
            if let Some(stop) = domain.stop.take() {
                let _ = stop.send(());
            }
        }
        let exits = self
            .domains
            .drain(..)
            .map(|d| {
                let ended = d.ended.into_inner().unwrap_or_else(|e| e.into_inner());
                let exit = ended.recv().unwrap_or(ChildExit::NotRunning);
                (d.name, exit)
            })
            .collect();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        exits
    }
}

impl Drop for Host {
    /// Stops the children, as [`Host::shutdown`] does, waiting for them.
    /// Dropped on a worker of a multi-threaded tokio runtime, it hands the
    /// worker's other tasks on while it waits
    /// (`tokio::task::block_in_place`).
    fn drop(&mut self) {
        let worker = tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
        match worker {
            true => tokio::task::block_in_place(|| self.stop_all()),
            false => self.stop_all(),
        };
    }
}

/// Refuses a child whose descriptor is of another kind than its `kind`
/// says ([`HostError::KindMismatch`]: a domain is an aggregate or a process
/// manager), and a policy or projector that names no input domain
/// ([`HostError::NoInput`]).
fn check_kind(
    domain: &str,
    kind: ComponentKindConfig,
    descriptor: &pb::ComponentDescriptor,
) -> Result<(), HostError> {
    let described = descriptor.kind();
    let agrees = match kind {
        ComponentKindConfig::Domain => matches!(
            described,
            pb::ComponentKind::Aggregate | pb::ComponentKind::ProcessManager
        ),
        ComponentKindConfig::Policy => described == pb::ComponentKind::Policy,
        ComponentKindConfig::Projector => described == pb::ComponentKind::Projector,
        ComponentKindConfig::Upcaster => false,
    };
    if !agrees {
        return Err(HostError::KindMismatch {
            domain: domain.to_string(),
            configured: kind.as_str().to_string(),
            described: described
                .as_str_name()
                .trim_start_matches("COMPONENT_KIND_")
                .to_string(),
        });
    }
    let stateless = matches!(
        kind,
        ComponentKindConfig::Policy | ComponentKindConfig::Projector
    );
    if stateless
        && descriptor
            .inputs
            .first()
            .is_none_or(|t| t.domain.is_empty())
    {
        return Err(HostError::NoInput {
            domain: domain.to_string(),
        });
    }
    Ok(())
}

/// Whether `described`, a restarted child's descriptor, is of the same
/// component as `registered`, the one its component was registered by: the
/// same name and kind, and for a policy or a projector (`kind`) the same
/// input domains and output domain, which its registration routes by.
fn same_component(
    kind: ComponentKindConfig,
    registered: &pb::ComponentDescriptor,
    described: &pb::ComponentDescriptor,
) -> bool {
    let inputs = |d: &pb::ComponentDescriptor| -> Vec<String> {
        d.inputs.iter().map(|t| t.domain.clone()).collect()
    };
    let routed = match kind {
        ComponentKindConfig::Policy | ComponentKindConfig::Projector => {
            inputs(registered) == inputs(described)
                && registered.output_domain == described.output_domain
        }
        ComponentKindConfig::Domain | ComponentKindConfig::Upcaster => true,
    };
    routed && registered.name == described.name && registered.kind == described.kind
}

/// Sends SIGTERM to the child `pid`.
fn terminate(pid: u32) {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return;
    };
    // SAFETY: kill(2) takes no pointers. The pid is a child this host has
    // not yet reaped (tokio's Child gives its id only until then), so it
    // names that child and no other process.
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
}

/// The wait after `backoff` when a restart fails again: twice as long, up
/// to `max`.
fn next_backoff(backoff: Duration, max: Duration) -> Duration {
    (backoff * 2).min(max)
}

/// The longest Unix socket path, in bytes: `sun_path`'s 108, less the NUL.
pub const MAX_SOCKET_PATH: usize = 107;

/// The longest name [`socket_path`] gives, with its leading `/`: a pid
/// (at most 7 digits on Linux) and a counter (at most 20).
const MAX_SOCKET_NAME: usize = "/angzarr--.sock".len() + 7 + 20;

/// Where the host puts its sockets: `configured`, refused when too long to
/// hold the socket names; else `xdg_runtime_dir` when it is absolute and
/// short enough; else `/tmp`.
pub(crate) fn socket_dir(
    configured: Option<&Path>,
    xdg_runtime_dir: Option<PathBuf>,
) -> Result<PathBuf, HostError> {
    let max = MAX_SOCKET_PATH - MAX_SOCKET_NAME;
    let fits = |dir: &Path| dir.as_os_str().len() <= max;
    match configured {
        Some(dir) if fits(dir) => Ok(dir.to_path_buf()),
        Some(dir) => Err(HostError::SocketDirTooLong {
            dir: dir.display().to_string(),
            max_bytes: max as u64,
        }),
        None => Ok(xdg_runtime_dir
            .filter(|dir| dir.is_absolute() && fits(dir))
            .unwrap_or_else(|| PathBuf::from("/tmp"))),
    }
}

/// A fresh socket path in `dir`: the host's pid and a counter, kept short
/// (at most [`MAX_SOCKET_NAME`] bytes after `dir`).
fn socket_path(dir: &Path) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("angzarr-{}-{n}.sock", std::process::id()))
}

/// A child that came up: handshaken, connected and described, or, a
/// sidecar, its health watched.
struct Up {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<BufReader<ChildStdout>>,
    channel: tonic::transport::Channel,
    descriptor: pb::ComponentDescriptor,
    socket: PathBuf,
    /// Whether it serves: a described child does; a sidecar as its health
    /// first said.
    serving: bool,
    /// A sidecar's health, each status it changes to; `None` for a
    /// described child, and once the watch has ended.
    health: Option<tonic::Streaming<HealthCheckResponse>>,
}

/// The next status `health` reports, `true` for SERVING; never, when it is
/// `None`. A watch that ends (or fails) is not serving, and is not watched
/// again: the sidecar stays down until its child is restarted.
async fn next_health(health: &mut Option<tonic::Streaming<HealthCheckResponse>>) -> bool {
    let Some(watch) = health else {
        return std::future::pending().await;
    };
    match watch.message().await {
        Ok(Some(response)) => response.status == ServingStatus::Serving as i32,
        _ => {
            *health = None;
            false
        }
    }
}

/// Where a test holds every supervisor, to force an interleaving rather
/// than wait for it: none outside the tests.
#[derive(Clone, Default)]
struct Hooks {
    /// Awaited by a supervisor once it has told `start` its first child is
    /// up, before it does anything else.
    #[cfg(test)]
    reported_up: Option<Arc<tokio::sync::Notify>>,
    /// Told, by a supervisor, each health status it has acted on: `true`
    /// once it has published its child's connection, `false` once it has
    /// withdrawn it.
    #[cfg(test)]
    applied: Option<mpsc::Sender<bool>>,
}

impl Hooks {
    async fn reported_up(&self) {
        #[cfg(test)]
        if let Some(hold) = &self.reported_up {
            hold.notified().await;
        }
    }

    #[cfg_attr(not(test), allow(unused_variables))]
    fn applied(&self, serving: bool) {
        #[cfg(test)]
        if let Some(applied) = &self.applied {
            let _ = applied.send(serving);
        }
    }
}

/// One remote component's supervisor: it starts the child, restarts it when it
/// exits, and stops it when told.
struct Supervisor {
    domain: String,
    kind: ComponentKindConfig,
    launch: Launch,
    supervision: Supervision,
    socket_dir: PathBuf,
    link: Arc<Link>,
    hooks: Hooks,
}

impl Supervisor {
    /// Supervises until `stop`; how the child ended. The first start's
    /// outcome goes to `up`; a first start that fails ends supervision.
    async fn run(
        self,
        mut stop: oneshot::Receiver<()>,
        up: mpsc::Sender<Result<pb::ComponentDescriptor, HostError>>,
    ) -> ChildExit {
        let mut up = Some(up);
        let initial = Duration::from_millis(self.supervision.initial_backoff_ms);
        let max = Duration::from_millis(self.supervision.max_backoff_ms);
        let mut backoff = initial;
        loop {
            let started = tokio::select! {
                started = self.come_up() => started,
                _ = &mut stop => return ChildExit::NotRunning,
            };
            let started = match started {
                // Every child is registered by its descriptor before it is
                // published: a restarted one is described again (C-0563).
                Ok(mut child) => match self.registered_by(&child.descriptor, up.is_some()) {
                    Ok(()) => Ok(child),
                    Err(error) => {
                        let _ = child.child.kill().await;
                        let _ = std::fs::remove_file(&child.socket);
                        Err(error)
                    }
                },
                Err(error) => Err(error),
            };
            match started {
                Ok(mut child) => {
                    backoff = initial;
                    // Published before `start` hears the child is up, so
                    // the handler it hands back reaches the child at once.
                    self.serve(child.serving.then_some(&child.channel));
                    if let Some(up) = up.take() {
                        let _ = up.send(Ok(child.descriptor.clone()));
                        self.hooks.reported_up().await;
                    }
                    // The child's stdout after its handshake is its own
                    // output: passed on to the host's stderr, and drained so
                    // the child never blocks on a full pipe.
                    if let Some(mut stdout) = child.stdout.take() {
                        tokio::spawn(async move {
                            let _ = tokio::io::copy(&mut stdout, &mut tokio::io::stderr()).await;
                        });
                    }
                    loop {
                        tokio::select! {
                            _ = child.child.wait() => {
                                self.set_channel(None);
                                let _ = std::fs::remove_file(&child.socket);
                                break;
                            }
                            _ = &mut stop => {
                                self.set_channel(None);
                                let exit = self.stop_child(&mut child).await;
                                let _ = std::fs::remove_file(&child.socket);
                                return exit;
                            }
                            serving = next_health(&mut child.health) => {
                                self.serve(serving.then_some(&child.channel));
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Some(up) = up.take() {
                        let _ = up.send(Err(error));
                        return ChildExit::NotRunning;
                    }
                    backoff = next_backoff(backoff, max);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = &mut stop => return ChildExit::NotRunning,
            }
        }
    }

    /// Registers a child by `descriptor`, its answer to `Describe` (an
    /// upcaster sidecar, which is not described, is left as it is): the
    /// `first` child's is the one its component is registered by; a
    /// restarted child's replaces it, its registered domain's descriptor
    /// replaced whole
    /// ([`Description::replace`](angzarr_embedded::Description::replace)),
    /// under the lock [`Host::register`] registers under, before the child
    /// is published. Refused, the old registration kept, when the registry
    /// refuses it ([`HostError::Register`]) or it describes another
    /// component than the first child did ([`HostError::DescriptorChanged`]):
    /// another name or kind, or a policy or projector of other inputs or
    /// another output domain, which the kernel routes by for good.
    fn registered_by(
        &self,
        descriptor: &pb::ComponentDescriptor,
        first: bool,
    ) -> Result<(), HostError> {
        if self.kind == ComponentKindConfig::Upcaster {
            return Ok(());
        }
        let mut registered = self.link.registered();
        if !first && !same_component(self.kind, &registered.descriptor, descriptor) {
            return Err(HostError::DescriptorChanged {
                domain: self.domain.clone(),
            });
        }
        if let Some(description) = &registered.description {
            description
                .replace(descriptor.clone())
                .map_err(|cause| HostError::Register {
                    cause: angzarr_diagnostic::Report::of(&cause),
                })?;
        }
        registered.descriptor = descriptor.clone();
        Ok(())
    }

    /// Publishes `channel`, the child up, counting it; or, `None`,
    /// withdraws the child's connection, the child down.
    fn serve(&self, channel: Option<&tonic::transport::Channel>) {
        self.set_channel(channel.cloned());
        if channel.is_some() {
            self.link.generation.send_modify(|n| *n += 1);
        }
        self.hooks.applied(channel.is_some());
    }

    fn set_channel(&self, channel: Option<tonic::transport::Channel>) {
        *self
            .link
            .channel
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = channel;
    }

    /// Closes the child's stdin and gives it the grace to exit; then sends
    /// it SIGTERM and gives it the terminate grace; then kills it.
    async fn stop_child(&self, up: &mut Up) -> ChildExit {
        drop(up.stdin.take());
        let grace = Duration::from_millis(self.supervision.shutdown_grace_ms);
        if let Ok(Ok(status)) = tokio::time::timeout(grace, up.child.wait()).await {
            return ChildExit::Exited(status.code());
        }
        if let Some(pid) = up.child.id() {
            terminate(pid);
        }
        let grace = Duration::from_millis(self.supervision.terminate_grace_ms);
        if let Ok(Ok(_)) = tokio::time::timeout(grace, up.child.wait()).await {
            return ChildExit::Terminated;
        }
        let _ = up.child.kill().await;
        ChildExit::Killed
    }

    /// Launches the child and brings it up. On a failure the child, if one
    /// started, is killed and reaped before the error is returned.
    async fn come_up(&self) -> Result<Up, HostError> {
        let domain = || self.domain.clone();
        let socket = socket_path(&self.socket_dir);
        let _ = std::fs::remove_file(&socket);
        let mut command = Command::new(&self.launch.program);
        command
            .args(&self.launch.args)
            .envs(&self.launch.env)
            .env(SOCKET_ENV, &socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if let Some(dir) = &self.launch.dir {
            command.current_dir(dir);
        }
        let mut child = command.spawn().map_err(|e| HostError::Spawn {
            domain: domain(),
            program: self.launch.program.clone(),
            kind: format!("{:?}", e.kind()),
        })?;
        let stdin = child.stdin.take();
        let mut stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
        let timeout = Duration::from_millis(self.supervision.handshake_timeout_ms);
        let reached = async {
            let handshake = read_handshake(&mut stdout, timeout)
                .await
                .map_err(|cause| HostError::Handshake {
                    domain: domain(),
                    cause,
                })?;
            let channel =
                connect_unix(handshake.socket.clone())
                    .await
                    .map_err(|_| HostError::Connect {
                        domain: domain(),
                        socket: handshake.socket.display().to_string(),
                    })?;
            // An upcaster sidecar has no descriptor (decision 14.39): it
            // serves UpcasterService and gRPC health, and is up while its
            // health, the whole server's, reports SERVING.
            if self.kind == ComponentKindConfig::Upcaster {
                let watch = HealthClient::new(channel.clone())
                    .watch(HealthCheckRequest::default())
                    .await
                    .map_err(|status| HostError::Health {
                        domain: domain(),
                        grpc_code: status.code() as i32,
                    })?
                    .into_inner();
                let mut health = Some(watch);
                let serving = next_health(&mut health).await;
                return Ok((
                    channel,
                    pb::ComponentDescriptor::default(),
                    handshake.socket,
                    serving,
                    health,
                ));
            }
            let descriptor = ComponentServiceClient::new(channel.clone())
                .describe(())
                .await
                .map_err(|status| HostError::Describe {
                    domain: domain(),
                    grpc_code: status.code() as i32,
                })?
                .into_inner();
            check_kind(&self.domain, self.kind, &descriptor)?;
            crate::validate(&descriptor).map_err(|cause| HostError::Descriptor {
                domain: domain(),
                cause,
            })?;
            Ok((channel, descriptor, handshake.socket, true, None))
        }
        .await;
        match reached {
            Ok((channel, descriptor, socket, serving, health)) => Ok(Up {
                child,
                stdin,
                stdout: Some(stdout),
                channel,
                descriptor,
                socket,
                serving,
                health,
            }),
            Err(error) => {
                let _ = child.kill().await;
                let _ = std::fs::remove_file(&socket);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "host.test.rs"]
mod tests;

#[cfg(test)]
#[path = "kernel.test.rs"]
mod kernel_tests;

#[cfg(test)]
#[path = "components.test.rs"]
mod component_tests;

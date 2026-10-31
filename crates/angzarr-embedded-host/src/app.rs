//! [`run`]: the whole application from its config, what the CLI's
//! generated `main.rs` calls.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), angzarr_embedded_host::HostError> {
//!     let app = std::sync::Arc::new(angzarr_embedded_host::run("angzarr.toml", register).await?);
//!     // app.handle(book, &sampled).await from any task, concurrently;
//!     // app.events(&cover).await, app.kernel(), ...
//!     app.shutdown().await; // or drop it: the children stop
//!     Ok(())
//! }
//! ```
//!
//! The [`App`] is `Send + Sync` and its methods are async, on the caller's
//! runtime: commands run concurrently, and nothing orders streams against
//! each other (ordering is per stream only). Two commands to one stream
//! race optimistically: both decide, the store takes one write, and the
//! loser is decided again on the new head, unless it set an expected
//! sequence, which makes the loss its business failure. Its store is ONE
//! SQLite connection, which serializes the I/O and nothing else.
//!
//! The host's children are started and stopped off the caller's workers
//! (`tokio::task::spawn_blocking`): the App has no runtime of its own, and
//! the caller's runtime is tokio's, with its timer.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;

use angzarr_diagnostic::Report;
use angzarr_embedded::v1::{CommandBook, Cover, EventBook, EventPage};
use angzarr_embedded::{
    EventStore, Failure, Handled, Kernel, KernelError, Malformed, MemoryDeadLetters, OnPolicyBug,
    OnRetryable, Registry, Rejected, Sampled, StoreError, StreamRef, Target,
};
use angzarr_router::pb::{CommandResponse, ComponentKind};
use angzarr_store_sqlite::SqliteStore;

use crate::config::{Expose, HostConfig, Languages};
use crate::error::HostError;
use crate::exposure::{Arrival, NotExposed};
use crate::host::{ChildExit, Host};

/// Runs the application the config at `config` describes: reads it
/// (TOML, strictly: [`HostConfig::from_toml`]), starts its remote
/// components ([`Host::start`]), registers them by kind and calls
/// `register` to register the in-process ones, holding what it registers
/// to the in-process `[[component]]`s, by name and kind (one listed and
/// not registered, [`HostError::NotRegistered`]; one registered and not
/// listed, [`HostError::NotListed`]), opens
/// its store (SQLite, `[store]`) on the caller's runtime, and resumes the
/// policy work its store still owes ([`Kernel::resume`]: what a crash left
/// between a commit and its policies' steps) before it returns. The [`App`] is
/// how the application sends commands and reads its streams; dropping it
/// stops the children. A component exposed (`expose`) that is registered
/// as a process manager is refused ([`HostError::NotExposable`]): only an
/// aggregate is served through the host's listener. On any refusal the
/// children started are stopped again. Its components' languages are the
/// built-in ones ([`Languages::builtin`]); [`run_with_languages`] loads
/// more.
pub async fn run(
    config: impl AsRef<Path>,
    register: impl FnOnce(&mut Registry),
) -> Result<App, HostError> {
    run_with_languages(config, Languages::builtin(), register).await
}

/// [`run`], its components' `language` one of `languages`: the built-in
/// ones and those the application loads before it starts
/// (`Languages::builtin().with(Language::child("go"))`). A component in a
/// language not among them is refused before anything starts
/// ([`HostError::UnknownLanguage`], its key path and the languages
/// loaded).
pub async fn run_with_languages(
    config: impl AsRef<Path>,
    languages: Languages,
    register: impl FnOnce(&mut Registry),
) -> Result<App, HostError> {
    let path = config.as_ref().to_path_buf();
    let (config, host, languages) = blocking(move || {
        let config = read(&path)?;
        let host = Host::start_with_languages(config.clone(), &languages)?;
        Ok((config, host, languages))
    })
    .await?;
    let mut registry = Registry::new();
    let registered = host
        .register(&mut registry)
        .map_err(|e| HostError::Register {
            cause: Report::of(&e),
        });
    if let Err(error) = registered {
        return Err(stopped(host, error).await);
    }
    let hosted = Registered::of(&registry);
    register(&mut registry);
    let local = Registered::of(&registry).since(&hosted);
    if let Err(error) = listed(&config, &languages, &local, &registry) {
        return Err(stopped(host, error).await);
    }
    let exposed: BTreeMap<String, Expose> = config
        .components
        .iter()
        .filter(|c| c.expose != Expose::None)
        .map(|c| (c.name.clone(), c.expose))
        .collect();
    if let Some(name) = exposed.keys().find(|name| process_manager(&registry, name)) {
        let error = HostError::NotExposable {
            domain: name.clone(),
            kind: PROCESS_MANAGER.to_string(),
        };
        return Err(stopped(host, error).await);
    }
    let opened = match config.store.in_memory {
        true => SqliteStore::open_in_memory().await,
        false => SqliteStore::open(&config.store.path).await,
    };
    let store = match opened {
        Ok(store) => store,
        Err(e) => {
            let error = HostError::Store {
                cause: Report::of(&KernelError::Store(e)),
            };
            return Err(stopped(host, error).await);
        }
    };
    let app = App {
        host,
        registry,
        dead_letters: MemoryDeadLetters::new(),
        policy_bug: config.policy.on_bug,
        retry: config.retry.on_retryable(),
        store,
        exposed,
    };
    app.resume().await;
    Ok(app)
}

/// What a resume at start is sampled at: the wall clock, and a UUIDv7 of
/// it (RFC 9562, its random bits zero), which a resume never uses: its
/// steps' command ids are derived from their triggers.
fn started_at() -> Sampled {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let ms = since.as_millis() as u64;
    let mut id = [0u8; 16];
    id[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    id[6] = 0x70;
    id[8] = 0x80;
    Sampled {
        now: angzarr_embedded::Timestamp {
            seconds: since.as_secs() as i64,
            nanos: since.subsec_nanos() as i32,
        },
        command_id: angzarr_embedded::v1::Uuid::from_bytes(id),
    }
}

/// The `kind` a process manager is refused exposure under: not a
/// `[[component]]` kind (a domain is an aggregate or a process manager),
/// its descriptor's kind.
const PROCESS_MANAGER: &str = "process_manager";

/// Whether the component `name` is registered here as a process manager:
/// a domain whose component name it is (decision 14.50), or a remote
/// domain the host registered under its `[[component]]` name.
fn process_manager(registry: &Registry, name: &str) -> bool {
    let is = |domain: &str| {
        matches!(
            registry.target(domain),
            Some(Target::Local(handler)) if handler.kind() == ComponentKind::ProcessManager
        )
    };
    is(name)
        || registry
            .domains()
            .any(|domain| registry.component_name(domain).as_deref() == Some(name) && is(domain))
}

/// The config at `path`, read as TOML.
fn read(path: &Path) -> Result<HostConfig, HostError> {
    let text = std::fs::read_to_string(path).map_err(|e| HostError::ConfigUnreadable {
        path: path.display().to_string(),
        kind: format!("{:?}", e.kind()),
    })?;
    HostConfig::from_toml(&text, &path.display().to_string())
}

/// What a registry holds by name: its domains (by component name,
/// decision 14.50), policies and projectors.
#[derive(Default)]
struct Registered {
    domains: BTreeSet<String>,
    policies: BTreeSet<String>,
    projectors: BTreeSet<String>,
}

impl Registered {
    /// What `registry` holds now.
    fn of(registry: &Registry) -> Registered {
        let names = |names: &mut dyn Iterator<Item = &str>| names.map(str::to_string).collect();
        Registered {
            domains: registry
                .domains()
                .filter_map(|domain| registry.component_name(domain))
                .collect(),
            policies: names(&mut registry.policy_names()),
            projectors: names(&mut registry.projector_names()),
        }
    }

    /// What `self` holds that `before` did not.
    fn since(self, before: &Registered) -> Registered {
        let new = |now: BTreeSet<String>, then: &BTreeSet<String>| {
            now.into_iter()
                .filter(|name| !then.contains(name))
                .collect()
        };
        Registered {
            domains: new(self.domains, &before.domains),
            policies: new(self.policies, &before.policies),
            projectors: new(self.projectors, &before.projectors),
        }
    }
}

/// The config's in-process components held to what `register`
/// registered in process, `local` (the registry's domains, policies and
/// projectors the host did not register), each by name and kind, a
/// domain by its component name (an aggregate's domain, a process
/// manager's `pmg-` and its domain: decision 14.50): a listed
/// one registered of no such kind and name is [`HostError::NotRegistered`]
/// (an upcaster's `upcasts` no domain registered at all), in the config's
/// order; then one registered that no in-process `[[component]]` of its
/// kind lists is [`HostError::NotListed`], domains first, then policies,
/// then projectors.
fn listed(
    config: &HostConfig,
    languages: &Languages,
    local: &Registered,
    registry: &Registry,
) -> Result<(), HostError> {
    use crate::config::{ComponentKindConfig as Kind, Placement};
    let in_process: Vec<_> = config
        .components
        .iter()
        .filter(|c| {
            languages
                .get(&c.language)
                .is_some_and(|language| matches!(c.placement(language), Ok(Placement::InProcess)))
        })
        .collect();
    for component in &in_process {
        let registered = match component.kind {
            Kind::Domain => local.domains.contains(&component.name),
            Kind::Policy => local.policies.contains(&component.name),
            Kind::Projector => local.projectors.contains(&component.name),
            Kind::Upcaster => component
                .upcasts
                .as_deref()
                .is_some_and(|domain| registry.domains().any(|d| d == domain)),
        };
        if !registered {
            return Err(HostError::NotRegistered {
                domain: component.name.clone(),
                kind: component.kind.as_str().to_string(),
            });
        }
    }
    let unlisted = |names: &BTreeSet<String>, kind: Kind| {
        names
            .iter()
            .find(|name| {
                !in_process
                    .iter()
                    .any(|c| c.kind == kind && &c.name == *name)
            })
            .map(|name| HostError::NotListed {
                domain: name.clone(),
                kind: kind.as_str().to_string(),
            })
    };
    match unlisted(&local.domains, Kind::Domain)
        .or_else(|| unlisted(&local.policies, Kind::Policy))
        .or_else(|| unlisted(&local.projectors, Kind::Projector))
    {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// `work`, which blocks (starting or stopping the host's children), run
/// off the caller's async workers. A panic in it is the caller's.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(done) => done,
        Err(error) => std::panic::resume_unwind(error.into_panic()),
    }
}

/// `work` awaited, each poll of it on a multi-threaded runtime's worker
/// made with the worker's other tasks handed on
/// (`tokio::task::block_in_place`). The kernel runs business code (a
/// domain's decide, a policy) synchronously as it is polled, and business
/// code may block; a task the worker held next (in its slot that no other
/// worker takes) would wait out the block, so commands for other streams,
/// which share nothing, would not run at once. On a current-thread
/// runtime, which has no other worker to hand on to, it is polled as is.
async fn off_worker<F: Future>(work: F) -> F::Output {
    let threaded = tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| match threaded {
        true => tokio::task::block_in_place(|| work.as_mut().poll(cx)),
        false => work.as_mut().poll(cx),
    })
    .await
}

/// `error`, once `host`'s children are stopped.
async fn stopped(host: Host, error: HostError) -> HostError {
    blocking(move || host.shutdown()).await;
    error
}

/// A running application: its registry, store, kernel and remote
/// components. `Send + Sync`: shared (an `Arc`) by every task that sends
/// it commands, and by the host's listener.
pub struct App {
    // Dropped in this order: the children stopped first.
    host: Host,
    registry: Registry,
    dead_letters: MemoryDeadLetters,
    /// The config's `[policy] on_bug`.
    policy_bug: OnPolicyBug,
    /// The config's `[retry]`.
    retry: OnRetryable,
    store: SqliteStore,
    /// Each `[[component]]` exposed to the host's listener, by name: one
    /// not here is exposed `"none"`.
    exposed: BTreeMap<String, Expose>,
}

impl App {
    /// The kernel over this application's registry and dead letters,
    /// doing with a policy's bug what the config's `[policy] on_bug` says,
    /// and retrying a step's retryable failure as its `[retry]` does.
    pub fn kernel(&self) -> Kernel<'_> {
        Kernel::new(&self.registry, &self.dead_letters)
            .on_policy_bug(self.policy_bug)
            .on_retryable(self.retry)
    }

    /// Runs the policy work the store still owes, by position
    /// ([`Kernel::resume`]), at [`started_at`]. A step that fails is as it
    /// is in any chain: dead-lettered, its position where `[policy] on_bug`
    /// leaves it, and a retryable one's held for the next resume. Its
    /// failure is logged here and does not stop the application from
    /// starting: the work stays the store's to owe.
    async fn resume(&self) {
        let kernel = self.kernel();
        let mut projections = ();
        let sampled = started_at();
        let resumed = off_worker(kernel.resume(&self.store, &sampled, &mut projections)).await;
        if let Err(failure) = resumed {
            eprintln!("{failure}");
        }
    }

    /// The application's store.
    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The host of the remote components.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// Handles one command book, for any domain, and every policy step it
    /// sets off ([`Kernel::handle_any`]), once the chain settles. Calls run
    /// concurrently; two on one stream race optimistically, as the kernel's
    /// do.
    // The kernel's own Failure, as Kernel::handle_any returns it.
    #[allow(clippy::result_large_err)]
    pub async fn handle(
        &self,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>> {
        let kernel = self.kernel();
        let mut projections = ();
        off_worker(kernel.handle_any(&self.store, book, sampled, &mut projections)).await
    }

    /// Every event of the stream `cover` names, in sequence order.
    pub async fn events(&self, cover: &Cover) -> Result<Vec<EventPage>, StoreError> {
        let stream = StreamRef::try_from(cover)?;
        Ok(self.store.read(stream, 0, None).await?.pages)
    }

    /// The exposure seam ([`crate::exposure`]): one request to the host's
    /// listener, which arrived from `arrived`. Handled as [`App::handle`]
    /// handles a book when its domain's `[[component]]` is exposed to the
    /// arrival, and answered as `CommandHandlerCoordinatorService.Handle`
    /// answers: the addressed stream's events and the delivery outcome.
    /// A domain not exposed to the arrival is refused as an unknown domain
    /// is (`Malformed::UnknownDomain`), the real reason logged locally
    /// only.
    // The kernel's own Failure, as Kernel::handle_any returns it.
    #[allow(clippy::result_large_err)]
    pub async fn serve(
        &self,
        book: CommandBook,
        arrived: Arrival,
        sampled: &Sampled,
    ) -> Result<CommandResponse, Failure<Rejected>> {
        let cover = book.cover.clone();
        if let Some(cover) = &cover {
            let expose = self.exposed.get(&cover.domain).copied().unwrap_or_default();
            if !arrived.admitted_by(expose) {
                let hidden = NotExposed {
                    domain: cover.domain.clone(),
                    arrival: arrived,
                    expose,
                };
                eprintln!("{hidden}");
                return Err(Failure::Kernel(KernelError::Malformed(
                    Malformed::UnknownDomain {
                        domain: cover.domain.clone(),
                    },
                )));
            }
        }
        let handled = self.handle(book, sampled).await?;
        let events = handled
            .books()
            .into_iter()
            .find(|book| book.cover == cover)
            .unwrap_or(EventBook {
                cover,
                ..EventBook::default()
            });
        Ok(CommandResponse {
            events: Some(events),
            outcome: Some(handled.outcome()),
        })
    }

    /// Stops the remote components' children ([`Host::shutdown`], off the
    /// caller's workers) and closes the store.
    pub async fn shutdown(self) -> Vec<(String, ChildExit)> {
        let App { host, store, .. } = self;
        let exits = blocking(move || host.shutdown()).await;
        drop(store);
        exits
    }
}

#[cfg(test)]
#[path = "app.test.rs"]
mod tests;

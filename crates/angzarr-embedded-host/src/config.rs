//! The host's config: EVERY component of the application, in-process and
//! remote, what kind each is, the language each is written in, and how
//! each remote one is launched. The CLI writes it beside the thin
//! `main.rs` it generates; its TOML form is
//!
//! ```toml
//! [[component]]
//! name = "table"
//! kind = "domain"              # required: domain, policy, projector or upcaster
//! language = "rust"            # in-process: registered by main.rs, not launched
//! expose = "loopback"          # reachable through the host's listener from
//!                              # this machine; "ip" from the network; "none"
//!                              # (the default) from nowhere
//!
//! [[component]]
//! name = "policy-table-player"
//! kind = "policy"              # in-process too: main.rs registers it
//! language = "rust"
//!
//! [[component]]
//! name = "player"
//! kind = "domain"
//! language = "python"          # launched as `uv run --locked -m player.serve`
//! entry = ["-m", "player.serve"]
//! dir = "domains/player"
//!
//! [[component]]
//! name = "table-player"
//! kind = "policy"              # a PolicyService child
//! language = "python"
//! entry = ["-m", "table_player.serve"]
//!
//! [[component]]
//! name = "dealer"
//! kind = "domain"
//! language = "go"              # loaded by the application, with no
//! launch = ["./bin/dealer"]    # default: the launch command is given
//!
//! [supervision]
//! handshake_timeout_ms = 10000
//!
//! [store]
//! path = "angzarr.db"          # SQLite; `in_memory = true` for none on disk
//!
//! [policy]
//! on_bug = "move_past"         # a policy step's bug, dead-lettered, moves its
//!                              # position past the trigger; "stay" keeps it
//!
//! [retry]
//! max_retries = 3              # a retryable failure is retried this often
//!                              # (0: never), then dead-lettered; waiting
//! max_race_runs = 10           # a command that keeps losing its write to
//!                              # another write to its stream runs this
//!                              # often, then is dead-lettered (0: never
//!                              # again, the conflict back to the caller,
//!                              # retryable, not dead-lettered); waiting
//! initial_backoff_ms = 100     # this long first,
//! multiplier = 2.0             # this many times longer each retry,
//! max_backoff_ms = 5000        # at most this long,
//! jitter = 0.2                 # less up to this fraction of it at random
//! ```
//!
//! A component's `kind` is required, with no default: `"domain"` (an
//! aggregate or a process manager; a child serves
//! `CommandHandlerService`), `"policy"` (`PolicyService`), `"projector"`
//! (`ProjectorService`) or `"upcaster"` (an upcaster, naming the domain it
//! `upcasts`; a child is a sidecar, decision 14.39, serving
//! `UpcasterService` and gRPC health). A child's descriptor must agree
//! with its kind ([`HostError::KindMismatch`]); a sidecar has none, and is
//! up while its health reports SERVING. An in-process component (Rust, no
//! `launch`) is registered by the application's `register`, and
//! [`run`](crate::run) holds the two to each other: one listed that
//! `register` did not register ([`HostError::NotRegistered`]), and one
//! registered that no `[[component]]` lists ([`HostError::NotListed`]),
//! are refused, by name and kind; an in-process upcaster's `upcasts` is a
//! domain registered here.
//!
//! The config is read strictly ([`HostConfig::from_toml`]): an unknown key
//! in any table, a missing required key, and a value outside a closed set
//! (`kind`, `expose`, `[policy] on_bug`) are refused, naming the TOML key
//! path where it goes wrong ([`HostError::ConfigInvalid`]). `language` is a
//! closed set too, populated by the languages loaded ([`Languages`]): the
//! built-in Rust (in process) and Python (a child, `uv run --locked`), and
//! any the application loads before it starts
//! ([`crate::run_with_languages`]); one not loaded is refused with its key
//! path and the languages loaded ([`HostError::UnknownLanguage`]). A loaded
//! language with no default launch command needs each component's
//! `launch`.
//!
//! ```toml
//! [[component]]
//! name = "table-upcaster"
//! kind = "upcaster"            # the table's inputs are upcast through it
//! upcasts = "table"
//! language = "python"
//! entry = ["-m", "table_upcaster.serve"]
//! ```
//!
//! A component's language picks where it runs and its default launch
//! command, and nothing else: the host speaks one protocol to every child
//! (the handshake, then gRPC on a Unix socket), so a language is loaded
//! with no more than its name ([`Language::child`]) and, if it has one, its
//! default launch ([`Language::launched_by`]).

use std::collections::BTreeMap;
use std::path::PathBuf;

use angzarr_embedded::{OnPolicyBug, OnRetryable};
use serde::{Deserialize, Serialize};

use crate::error::HostError;

/// The built-in language whose components run in the application's own
/// process, as angzarr-embedded components: the host launches none of them
/// unless the config gives one a `launch`.
pub const IN_PROCESS_LANGUAGE: &str = "rust";

/// A language a component may be written in, as the host knows it: its
/// name (a `[[component]]`'s `language`) and where a component in it runs
/// when the config gives no `launch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Language {
    name: String,
    runs: Runs,
}

/// Where a component of a [`Language`] with no `launch` runs.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Runs {
    /// In the application's process, registered by its `register`.
    InProcess,
    /// As a child, launched by this command (program first) followed by the
    /// component's `entry`; with none, every component needs its `launch`.
    Child(Option<Vec<String>>),
}

impl Language {
    /// A language whose components with no `launch` run in process,
    /// registered by the application's `register`.
    pub fn in_process(name: impl Into<String>) -> Language {
        Language {
            name: name.into(),
            runs: Runs::InProcess,
        }
    }

    /// A language whose components run as children, each launched by its
    /// own `launch` until [`Language::launched_by`] gives a default.
    pub fn child(name: impl Into<String>) -> Language {
        Language {
            name: name.into(),
            runs: Runs::Child(None),
        }
    }

    /// This language with `command` (program first) as its default launch,
    /// before a component's `entry`. A child language.
    pub fn launched_by<W: Into<String>>(self, command: impl IntoIterator<Item = W>) -> Language {
        Language {
            name: self.name,
            runs: Runs::Child(Some(command.into_iter().map(Into::into).collect())),
        }
    }

    /// Its name, as a `[[component]]`'s `language` names it.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The languages the host knows: a closed set, the built-in ones
/// ([`Languages::builtin`]) and those the application loads before it
/// starts ([`Languages::with`]; [`crate::run_with_languages`]). A
/// `[[component]]` whose `language` is not among them is refused
/// ([`HostConfig::check_languages`]). Its default is the built-in ones.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Languages(BTreeMap<String, Language>);

impl Languages {
    /// The host's own: Rust ([`IN_PROCESS_LANGUAGE`]), in process, and
    /// Python, a child launched by `uv run --locked` (the component's
    /// project with exactly its lock file's dependencies).
    pub fn builtin() -> Languages {
        Languages(BTreeMap::new())
            .with(Language::in_process(IN_PROCESS_LANGUAGE))
            .with(Language::child("python").launched_by(["uv", "run", "--locked"]))
    }

    /// These and `language`, which replaces one of its name.
    pub fn with(mut self, language: Language) -> Languages {
        self.0.insert(language.name.clone(), language);
        self
    }

    /// The language named `name`, if loaded.
    pub fn get(&self, name: &str) -> Option<&Language> {
        self.0.get(name)
    }

    /// The loaded languages' names, in order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

impl Default for Languages {
    fn default() -> Languages {
        Languages::builtin()
    }
}

/// The host's whole config. Every table refuses a key it does not know.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    /// Every component of the application, in-process and remote: the
    /// `[[component]]` tables.
    #[serde(rename = "component", default)]
    pub components: Vec<ComponentConfig>,
    /// How the host waits for, restarts and stops its children.
    #[serde(default)]
    pub supervision: Supervision,
    /// The store [`crate::run`] opens.
    #[serde(default)]
    pub store: StoreConfig,
    /// What the kernel does when a policy's step hits a bug.
    #[serde(default)]
    pub policy: PolicyConfig,
    /// How the kernel retries a step's retryable failure.
    #[serde(default)]
    pub retry: RetryConfig,
}

impl HostConfig {
    /// `text`, a config's TOML, read strictly: malformed TOML, an unknown
    /// key in any table, a missing required key (a component's `name`,
    /// `language` or `kind`), a value outside a closed set (`kind`,
    /// `expose`, `[policy] on_bug`) or of the wrong type is refused as
    /// [`HostError::ConfigInvalid`], naming `path`, the TOML key path
    /// where it goes wrong (`component[1].kind`, `retry.retries`; a
    /// missing key's table, `component[0]`; empty for TOML that does not
    /// parse), the byte it starts at, and the TOML reader's text.
    pub fn from_toml(text: &str, path: &str) -> Result<HostConfig, HostError> {
        toml::from_str(text).map_err(|error| {
            let offset = error.span().map_or(0, |span| span.start);
            HostError::ConfigInvalid {
                path: path.to_string(),
                key: key_at(text, offset),
                offset: offset as u64,
                ext_err_txt: error.message().to_string(),
            }
        })
    }

    /// Whether every component's `language` is one of `languages`: the
    /// first that is not is refused as [`HostError::UnknownLanguage`],
    /// naming its key path (`component[1].language`) and the languages
    /// loaded.
    pub fn check_languages(&self, languages: &Languages) -> Result<(), HostError> {
        (0..self.components.len())
            .try_for_each(|index| self.language_of(index, languages).map(|_| ()))
    }

    /// The language of component `index` among `languages`, or
    /// [`HostError::UnknownLanguage`] naming its key path.
    pub(crate) fn language_of<'l>(
        &self,
        index: usize,
        languages: &'l Languages,
    ) -> Result<&'l Language, HostError> {
        let component = &self.components[index];
        languages
            .get(&component.language)
            .ok_or_else(|| HostError::UnknownLanguage {
                key: format!("component[{index}].language"),
                language: component.language.clone(),
                loaded: languages.names().collect::<Vec<_>>().join(","),
            })
    }
}

/// The TOML key path, in `text`, of the deepest key or value holding byte
/// `offset`: `table.key`, an array's element by its index (`component[1]`).
/// A table's span is its header's alone, so every key is searched, not
/// only those within a table holding `offset`. Empty where `text` does
/// not parse, or no key holds it.
fn key_at(text: &str, offset: usize) -> String {
    let Ok(document) = toml::de::DeTable::parse(text) else {
        return String::new();
    };
    let mut found = (0, String::new());
    in_table(document.get_ref(), offset, &mut Vec::new(), &mut found);
    found.1
}

/// [`key_at`] within `table`, at `path`: the deepest match so far is
/// `found`, its depth and its path.
fn in_table(
    table: &toml::de::DeTable<'_>,
    offset: usize,
    path: &mut Vec<String>,
    found: &mut (usize, String),
) {
    for (key, value) in table {
        let segment = match path.is_empty() {
            true => key.get_ref().to_string(),
            false => format!(".{}", key.get_ref()),
        };
        path.push(segment);
        let held = key.span().contains(&offset) || value.span().contains(&offset);
        in_value_of(value.get_ref(), offset, held, path, found);
        path.pop();
    }
}

/// [`key_at`] at `value`, whose key or span holds `offset` when `held`:
/// itself, then a table's keys and an array's elements.
fn in_value_of(
    value: &toml::de::DeValue<'_>,
    offset: usize,
    held: bool,
    path: &mut Vec<String>,
    found: &mut (usize, String),
) {
    if held && path.len() >= found.0 {
        *found = (path.len(), path.concat());
    }
    match value {
        toml::de::DeValue::Table(table) => in_table(table, offset, path, found),
        toml::de::DeValue::Array(array) => {
            for (index, element) in array.iter().enumerate() {
                path.push(format!("[{index}]"));
                let held = element.span().contains(&offset);
                in_value_of(element.get_ref(), offset, held, path, found);
                path.pop();
            }
        }
        _ => {}
    }
}

/// The `[retry]` table: how the kernel retries a step whose failure is
/// retryable (UNAVAILABLE, ABORTED, UNIMPLEMENTED, DEADLINE_EXCEEDED,
/// RESOURCE_EXHAUSTED and the like), with exponential backoff and jitter,
/// before it dead-letters it. The kernel's
/// [`OnRetryable`](angzarr_embedded::OnRetryable), whose defaults
/// these are. Retrying is on with no config; `max_retries = 0` turns it
/// off. Every key has a default, so the table may be left out.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetryConfig {
    /// Retries after the first attempt, then the step is dead-lettered; 0
    /// turns retrying off.
    pub max_retries: u32,
    /// Runs of a command stamped with the head it loaded while it loses its
    /// write to another write to its stream, the first included: the race
    /// re-run cap (decision 14.46), then it is dead-lettered. 0 turns race
    /// re-runs off: the first lost race goes back to the caller as the
    /// store's conflict, retryable, and is not dead-lettered.
    pub max_race_runs: u32,
    /// The wait before the first retry.
    pub initial_backoff_ms: u64,
    /// What each retry's wait is the last one's times.
    pub multiplier: f64,
    /// The longest wait between retries.
    pub max_backoff_ms: u64,
    /// The most of each wait taken off at random, as a fraction of it (0
    /// to 1).
    pub jitter: f64,
}

impl Default for RetryConfig {
    /// The kernel's own defaults.
    fn default() -> Self {
        let kernel = OnRetryable::default();
        RetryConfig {
            max_retries: kernel.max_retries,
            max_race_runs: kernel.max_race_runs,
            initial_backoff_ms: millis(kernel.initial_backoff),
            multiplier: kernel.multiplier,
            max_backoff_ms: millis(kernel.max_backoff),
            jitter: kernel.jitter,
        }
    }
}

/// `duration` in whole milliseconds.
fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl RetryConfig {
    /// As the kernel's [`OnRetryable`].
    pub fn on_retryable(&self) -> OnRetryable {
        OnRetryable {
            max_retries: self.max_retries,
            max_race_runs: self.max_race_runs,
            initial_backoff: std::time::Duration::from_millis(self.initial_backoff_ms),
            multiplier: self.multiplier,
            max_backoff: std::time::Duration::from_millis(self.max_backoff_ms),
            jitter: self.jitter,
        }
    }
}

/// The `[policy]` table: how the kernel treats a policy whose step hits a
/// bug. Every key has a default, so the table may be left out.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PolicyConfig {
    /// What a policy step that hits a bug (the policy's own error, or a
    /// handler's bug in the step) does with the policy's position once the
    /// step is dead-lettered: `"move_past"` the trigger (the default: the
    /// dead letter is the record to replay) or `"stay"` at it, so a resume
    /// runs the step again. The kernel's
    /// [`OnPolicyBug`](angzarr_embedded::OnPolicyBug).
    #[serde(with = "OnBug")]
    pub on_bug: OnPolicyBug,
}

/// [`OnPolicyBug`]'s TOML form: `"move_past"` or `"stay"`.
#[derive(Serialize, Deserialize)]
#[serde(remote = "OnPolicyBug", rename_all = "snake_case")]
enum OnBug {
    MovePast,
    Stay,
}

/// The `[store]` table: the application's SQLite store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    /// The SQLite file, created when absent; relative to the working
    /// directory. `angzarr.db` by default.
    pub path: PathBuf,
    /// An in-memory SQLite store instead, gone when the application ends;
    /// `path` is then unused.
    pub in_memory: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            path: PathBuf::from("angzarr.db"),
            in_memory: false,
        }
    }
}

/// One `[[component]]` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentConfig {
    /// The component's name. An in-process domain's is its component name
    /// (decision 14.50): an aggregate's domain, a process manager's `pmg-`
    /// and its domain. A remote domain is registered as the domain this
    /// names.
    pub name: String,
    /// The language it is written in: one of the languages loaded
    /// ([`Languages`]: `"rust"`, `"python"`, and any the application
    /// loads), lowercase.
    pub language: String,
    /// What it is, required: `"domain"` (an aggregate or a process
    /// manager; a child serves `CommandHandlerService`), `"policy"`
    /// (`PolicyService`), `"projector"` (`ProjectorService`) or
    /// `"upcaster"` (a child is a sidecar, serving `UpcasterService` and
    /// gRPC health).
    pub kind: ComponentKindConfig,
    /// An upcaster's: the domain whose payloads it upcasts (decision
    /// 14.39); unset for every other kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upcasts: Option<String>,
    /// The command that starts its child, program first. Overrides the
    /// language's default (and its `entry`) entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch: Option<Vec<String>>,
    /// The arguments after the language's default launch command: the
    /// module or entry point (`["-m", "player.serve"]`, `["player-serve"]`).
    /// Unused when `launch` is given.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entry: Vec<String>,
    /// The child's working directory; the application's when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Environment variables set for the child, beside the host's own and
    /// `ANGZARR_SOCKET`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Who may send it commands from outside the application, through the
    /// host's one listener (`CommandHandlerCoordinatorService`, routed by
    /// `cover.domain`, this component's `name`): nobody (`"none"`, the
    /// default), this machine only (`"loopback"`) or the network
    /// (`"ip"`). Only an aggregate may be exposed
    /// ([`HostError::NotExposable`]).
    #[serde(default, skip_serializing_if = "Expose::is_none")]
    pub expose: Expose,
}

/// A `[[component]]` table's `expose`: from how far a command may reach
/// the component through the host's listener. Ordered: an exposure admits
/// every request a narrower one does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expose {
    /// Not reachable from outside the application.
    #[default]
    None,
    /// Reachable from this machine: a request arriving on loopback.
    Loopback,
    /// Reachable from the network: a request arriving on any address.
    Ip,
}

impl Expose {
    fn is_none(&self) -> bool {
        *self == Expose::None
    }

    /// Its TOML form: `"none"`, `"loopback"` or `"ip"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Expose::None => "none",
            Expose::Loopback => "loopback",
            Expose::Ip => "ip",
        }
    }
}

/// A `[[component]]` table's `kind`, required: what the component is, and
/// for a child, which of angzarr's component services it serves. A
/// child's descriptor's kind must agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentKindConfig {
    /// An aggregate or a process manager (`CommandHandlerService`); which,
    /// its descriptor says.
    Domain,
    /// A policy (`PolicyService`).
    Policy,
    /// A projector (`ProjectorService`).
    Projector,
    /// An upcaster sidecar (`UpcasterService` and gRPC health, decision
    /// 14.39) for the domain its component `upcasts`: no descriptor, no
    /// coordinator of its own.
    Upcaster,
}

impl ComponentKindConfig {
    /// Its TOML form: `"domain"`, `"policy"`, `"projector"` or
    /// `"upcaster"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ComponentKindConfig::Domain => "domain",
            ComponentKindConfig::Policy => "policy",
            ComponentKindConfig::Projector => "projector",
            ComponentKindConfig::Upcaster => "upcaster",
        }
    }
}

/// How the host supervises its children. Every field has a default, so the
/// `[supervision]` table, and any key in it, may be left out.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Supervision {
    /// How long a child has, from its start, to print its handshake.
    pub handshake_timeout_ms: u64,
    /// The wait before the first restart of a child that exited; each
    /// restart that fails doubles it, up to `max_backoff_ms`, and a child
    /// that comes up resets it.
    pub initial_backoff_ms: u64,
    /// The longest wait between restarts.
    pub max_backoff_ms: u64,
    /// How long a child has, once its stdin is closed at shutdown, to exit
    /// before it is sent SIGTERM.
    pub shutdown_grace_ms: u64,
    /// How long a child has, once sent SIGTERM, to exit before it is
    /// killed (SIGKILL).
    pub terminate_grace_ms: u64,
    /// How long a dispatch waits for the child's answer; past it, the call
    /// fails retryably (`cause = timeout`).
    pub call_timeout_ms: u64,
    /// The directory the host puts the children's sockets in. When absent,
    /// `$XDG_RUNTIME_DIR` if it is set, absolute and short enough, else
    /// `/tmp`: never `$TMPDIR`, which may be deep. A Unix socket's path is
    /// at most 107 bytes, so a directory too long to hold the host's socket
    /// names is refused at start ([`HostError::SocketDirTooLong`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_dir: Option<PathBuf>,
}

impl Default for Supervision {
    fn default() -> Self {
        Supervision {
            handshake_timeout_ms: 30_000,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
            shutdown_grace_ms: 5_000,
            terminate_grace_ms: 2_000,
            call_timeout_ms: 30_000,
            socket_dir: None,
        }
    }
}

/// Where a component runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    /// In the application's process: main.rs registers it; the host does
    /// nothing with it.
    InProcess,
    /// In a child process the host launches.
    Remote(Launch),
}

/// A child's resolved launch: what [`ComponentConfig::placement`] makes of the
/// config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Launch {
    pub program: String,
    pub args: Vec<String>,
    pub dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

impl ComponentConfig {
    /// Where this component, written in `language`, runs and, when remote,
    /// how it is launched: its `launch`, else, for an in-process language,
    /// in process, and for a child language its default followed by its
    /// `entry`. Refused when the launch is empty, or when no launch is
    /// given for a child language with no default.
    pub fn placement(&self, language: &Language) -> Result<Placement, HostError> {
        let words: Vec<String> = match (&self.launch, &language.runs) {
            (Some(launch), _) => launch.clone(),
            (None, Runs::InProcess) => return Ok(Placement::InProcess),
            (None, Runs::Child(Some(default))) => default
                .iter()
                .cloned()
                .chain(self.entry.iter().cloned())
                .collect(),
            (None, Runs::Child(None)) => {
                return Err(HostError::NoLaunch {
                    domain: self.name.clone(),
                    language: self.language.clone(),
                })
            }
        };
        let mut words = words.into_iter();
        let Some(program) = words.next() else {
            return Err(HostError::EmptyLaunch {
                domain: self.name.clone(),
            });
        };
        Ok(Placement::Remote(Launch {
            program,
            args: words.collect(),
            dir: self.dir.clone(),
            env: self.env.clone(),
        }))
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;

# Working rules

## Docs and code state the current design only

Version control holds the history; the docs and code do not. When a decision
changes, rewrite the text in place to say what is true now.

- No strikethroughs, "revised", "superseded", "originally", "previously",
  "moved to", "was X, now Y", or dated decision notes.
- No sections kept "for reference" after the design moved on — delete them.
- No code comments about what the code used to do.
- A rejected alternative may stay only as rationale for the current design
  ("not X, because …"), never as a story of how we got here.

## Few dependencies, no domain

angzarr-embedded is a deterministic application engine on CQRS/ES. Every crate here
knows nothing about any application that uses it, so anything specific to
one belongs in that application's repository, not here.

Every crate's dependencies are named below, each with why. A crate may
depend on another crate in this workspace, as angzarr-embedded-catalog-check does.

angzarr-embedded-catalog-check is test-only: angzarr-diagnostic's `catalog`
module checks the Fluent catalogs against the errors for the parity test,
and its one third-party dev-dependency, `fluent-bundle` (Apache-2.0 OR
MIT), renders each message with its error's args.
Nothing depends on it and it has no library to speak of.

angzarr-embedded depends on `angzarr-proto` (a git dependency pinned by rev:
angzarr's own prost types for `io.angzarr.v1`, whose build needs `protoc`),
`prost` and `prost-types`, because its event-sourcing kernel (`angzarr_embedded`)
operates on angzarr's wire types directly rather than on parallel angzarr-embedded
types: angzarr owns that contract, and angzarr-embedded is its in-process runtime.
Every user of angzarr-embedded therefore builds angzarr-proto; this is accepted.
angzarr-embedded also depends on `angzarr-ports`, pinned at the same rev of
angzarr core (`crates/angzarr-ports`, moved there from angzarr-project with
angzarr-proto): angzarr's shared store contract (one async
trait set, its errors and codes, its conformance suite), which angzarr-embedded's
kernel runs on and has no store traits of its own beside, and on
`angzarr-diagnostic`, at that rev too (`crates/angzarr-diagnostic`):
the `Diagnostic` trait every error here implements, re-exported as
`angzarr_embedded::angzarr_diagnostic`.
angzarr-embedded also depends on `angzarr-router`, a git dependency pinned by
rev: angzarr's shared client router, through which the kernel dispatches
business (routing by type, folding the state it is given, the handlers,
typed refusals and the conflict report), so those rules are implemented
once for every runtime. Everything else is coordinator work, and the
kernel's (decisions 14.39 to 14.42): the check, identification, upcasting
through the domain's upcaster port, snapshots, admission (deduplication,
the refusal fact's stream, a process manager's start rule from its
descriptor's starting types), conflict judgement, a policy's output checks,
the pass-through refusal fact and its address, a projector's failure
classes, classification, the coordinator's reason keys (`angzarr_embedded::reasons`,
angzarr-project's `CoordinatorReason` in `reasons.proto`), the store writes,
positions and resume. Policies reach it as the router's
`PolicyComponent`, so a policy in another process runs as an in-process
one does.

angzarr-embedded-macros depends on `syn`, `quote` and `proc-macro2`, because it is a
procedural macro crate that parses a whole impl (its methods' signatures,
attributes and types): hand-rolling a Rust parser over `proc_macro` tokens
would be a second, unchecked implementation of Rust's grammar. They are
compile-time only, and already in the graph through prost-derive, so they
add no crate: each is pinned with `=` to the version prost-derive resolves
in Cargo.lock, and moves when that does. Nothing else in the workspace
depends on them directly; the kernel (angzarr-embedded) never does. angzarr-embedded itself
depends on angzarr-embedded-macros only as a dev-dependency, for its decorated
aggregate test.

angzarr-store-sqlite depends on `angzarr-ports`, `sea-query`, `sqlx` and
`sea-query-binder`, because it is a SQL store: hand-rolling a query builder
or a driver would be a second, unchecked implementation of the dialect. It
is the one SQLite adapter angzarr-embedded and angzarr share, so it
is a LEAF: it depends on nothing of angzarr-embedded (its own test,
`a_store_depends_on_nothing_of_angzarr_embedded`, holds it to that), and its errors
are `Diagnostic`s through angzarr-ports' re-export of angzarr-diagnostic. It lives here
for the POC, beside its first user; it can move to angzarr-project unchanged.
angzarr-embedded depends on it the other way only optionally, behind its
`sqlite` feature (off by default, so no application, and not the wasm build,
gets SQLite unasked), to re-export it as
`angzarr_embedded::angzarr_store_sqlite`; it re-exports angzarr-proto,
angzarr-ports, angzarr-diagnostic and angzarr-router at its root too, so an
application pins angzarr-embedded alone.

angzarr-embedded-host depends on `angzarr-embedded` (the kernel's `Registry`,
which `Host::register` fills), `angzarr-router` and `angzarr-proto`
(pinned at the revs the router names, so its graph holds one angzarr-proto),
`tonic` and `tonic-prost` (its gRPC client, and in its tests the fake
child's server), `tokio`,
`tower` and `hyper-util` (the runtime and the Unix-socket connector),
`libc` (SIGTERM to a child at shutdown, which tokio cannot send),
`angzarr-store-sqlite` (the store `run` opens), and `serde` and `toml`
(its config, the angzarr.toml the CLI writes, whose `[[component]]` tables
name each component's `kind`: domain, policy, projector or upcaster), because a remote
component (a domain, a policy or a projector) is a child process speaking
gRPC on a Unix socket: hand-rolling HTTP/2 would be a second, unchecked
implementation of gRPC. Its stubs are written by `tonic-build`'s manual
builder from the router's Rust types, so no `io.angzarr.v1` message is
generated twice. Nothing else in the workspace depends on it but the catalog
check's tests.

Protobuf code lives in angzarr-embedded (angzarr's types) and
angzarr-embedded-host (the service stubs); every other crate stays
dependency-free except as above.

## Licence: Apache-2.0 OR BSD-3-Clause; not published

Licensed under either of Apache License 2.0 or BSD 3-Clause, at your option
(`LICENSE-APACHE`, `LICENSE-BSD`).
Every crate has `license = "Apache-2.0 OR BSD-3-Clause"` and `publish = false`, both inherited
from `[workspace.package]`. A new crate sets `license.workspace = true` and
`publish.workspace = true`.

## Errors are a key and args: a code, args, and an en-US message

Every public error type implements `angzarr_diagnostic::Diagnostic`, and
its `Display` is its code then its args (` name=value` each, a wrapped
cause flattened as `name=<code>` and `name.<arg>=value`, a value that is
empty or holds whitespace, `=`, `"` or `\` double-quoted with its `"` and
`\` escaped), never English (angzarr decision 14.37):
`angzarr_diagnostic::display` writes it.
English is rendered only where an error is shown, from the Fluent catalogs
keyed by the code: the game client renders it; angzarr-embedded renders nothing. The trait lives in
angzarr core's angzarr-diagnostic, not in angzarr-embedded, so angzarr's
shared crates (angzarr-ports, angzarr-yaml) and the apps share it without
depending on the event-sourcing kernel.

angzarr-embedded forwards angzarr-ports' `StoreError` under its own
`angzarr-ports-*` code through `KernelError::Store`; its messages are
angzarr-ports' own catalog, `angzarr_ports::EN_US` (re-exported as
`angzarr_embedded::angzarr_ports`), checked against its errors in core.

- **Codes are stable and never reused.** Kebab-case, starting with the
  crate's name (`angzarr-embedded-host-handshake-malformed`), and a valid Fluent message id. A code
  whose meaning changes gets a new code; a retired code is not given to
  anything else. The one exception was ruled once: the kernel's
  `angzarr-embedded-next-*` codes lost their `next-` (docs/design/audit.md,
  *The one rename of the kernel's codes*).
- **Args carry data, never pre-formatted English.** Numbers, ids, paths and
  the text that was read are args; any phrase a player reads belongs in the
  catalog. A wrapped cause is `Arg::Nested`. An error does not hold a
  `String` of prose: a new failure is a new variant with data fields.
- **A third party's error is carried as it said it.** Converting a
  library's error (the SQLite driver's, prost's, tonic's transport), its
  text goes verbatim into the arg `ext_err_txt`, beside its numeric code
  (`errno`) where it has one. A user handler's text (an escaped error's, a
  panic's) is never captured: it stays in the handler's own log.
- **A new variant needs a code and an en-US message** in
  `crates/<crate>/locales/en-US/<crate>.ftl`, using exactly the variant's
  args, with a Fluent selector wherever a count is shown. The catalog check
  (angzarr-embedded-catalog-check, on angzarr-diagnostic's `catalog::check`)
  holds the two to each other.
- A game's error that an angzarr-embedded error carries (a migration step's, a content
  decoder's) is held as an `angzarr_diagnostic::Report`, keeping the game's
  own code.
- Where a game's error crosses into the engine, the trait demands it:
  `Domain::Rejection: Diagnostic`, so a domain whose rejections are not
  localizable does not compile.

## Unicode in source is visible

The workspace denies three clippy lints, and every crate opts in with
`[lints] workspace = true`: `invisible_characters`, `unicode_not_nfc` and
`non_ascii_literal`. A non-ASCII character in a literal is written as an
escape (`"\u{e9}"`), so what the code holds is plain. There is no allow; a
new one needs a comment saying why.

## Tests live beside the code, in `*.test.rs`

A source file `foo.rs` keeps its tests in `foo.test.rs` in the same
directory, included at the end of `foo.rs`:

```rust
#[cfg(test)]
#[path = "foo.test.rs"]
mod tests;
```

Not an inline `mod tests { ... }`, and no `tests/` directory: the tests stay
next to what they test, and out of the source file, so reading the code does
not put its tests in the reader's (or an LLM's) context.

- Shared fixtures are `*.test.rs` files too, declared
  `#[cfg(test)] #[path = "toys.test.rs"] pub(crate) mod toys;`
  (`angzarr_embedded::toys`).
- A source file with several test files includes each,
  `#[cfg(test)] #[path = "edit_map.test.rs"] mod edit_map_tests;`, or keeps
  one `foo.test.rs` with a module per subject.
- A test reaches the crate as `crate::`, its private items included.
- Library-source scans (the catalog parity's code-literal check) skip
  `*.test.rs`.

## Use `just`

Plain `cargo build` and `cargo test` work from a clean checkout; angzarr-embedded's
build also needs `protoc` and github.com (for angzarr-proto).
`just wasm` builds angzarr-embedded for `wasm32-unknown-unknown`; it must keep
building there.

Every gate prints one line on a pass, `ok   <gate> (<scope>) <duration>`;
on a failure, a `FAIL` line, the extract worth reading, and the full log in
`target/logs/<gate>.log`. `VERBOSE=1 just <gate>` shows the full output.
`just test` runs cargo-nextest.

## The git-hook gates are in gates/; the justfile gives their parameters

The gates and their recipes are `gates/gates.just` and
`gates/lefthook.yml`: the justfile imports the one and lefthook.yml extends
the other. The justfile keeps angzarr-embedded's parameters for them
(`mutation_thresholds`, `workspace_wide`, `staged_tree`, `feature_rules`,
`features_prepare`) and its own `lint`; their tool configs are
`deny.toml`, `.gitleaks.toml` and `.cargo/mutants.toml`. `just
hooks-install` installs the hooks, once per clone (every worktree shares
them), and again after a change to the gates names a new hook
(`gates/MIGRATING.md`). They came from the shared gates repository that
taskloom and ctxloom-rs also copied; a change here does not reach them.

lefthook runs the gates one at a time, stopping at the first failure.
Gates 1 to 5 run at pre-commit, and pre-push runs the push's tests (`just
push-check`). Mutation (6) runs in no git hook: it runs while tests are
written and judged, at the end of an unattended run, and in the scheduled CI
full pass. A
failing gate is a finding: fix the cause and commit again; never
`--no-verify`, and never weaken a gate to get through.

1. **fmt** formats the staged `*.rs` files in the index (`just
   fmt-staged`); it does not refuse. A fully staged file's working copy gets
   the same content; a partially staged file's working copy is left exactly
   as it was, and named. `just fmt-check` is CI's check.
2. **secrets**: gitleaks over the staged changes (`.gitleaks.toml`). A fake
   secret in a test ends its line with `gitleaks:allow`.
3. **clippy**: `just lint`: clippy over the workspace.
4. **deps**: `just lint-deps`. A new licence, git source or advisory ignore
   is a change to `deny.toml`, with its reason.
5. **tests**: `just test-commit` runs `just test-staged`, which tests the
   crates with staged changes and their workspace dependents (normal, dev
   and build), on a throwaway worktree of HEAD plus the staged diff
   (`target/staged-tree`), so unstaged edits cannot pass or fail it. Red
   tests (failing tests, not a failed build) are committed on a branch other
   than the default one, with a warning that it cannot merge until they
   pass, and refused on the default branch.
6. **mutants** (no hook; by hand and in scheduled CI): `just mutants-push`
   runs `cargo mutants --in-diff` over what each ref given on stdin, as
   git's pre-push refs, adds (the diff from the remote's
   tip of that ref, or for a new branch from its merge base with the
   default branch, to the commit pushed), in that commit's tree, and holds
   every crate's new code to a 90% kill rate (`mutation_thresholds`; a new
   crate gets a threshold there before it gets code). Red tests bound for a
   branch other than the default one are not judged and pass with a
   warning. The last run's mutants.out is kept in
   `target/logs/mutants-push/`. `just mutants-staged` makes the same
   judgement over the staged diff, by hand, and `just mutants-all` over the
   whole workspace at HEAD (the full pass; weekly in CI,
   `.github/workflows/mutation.yml`). A mutant judged equivalent is excluded by
   an `exclude_re` in `.cargo/mutants.toml` matching that one mutant, with a
   comment saying why: not `#[mutants::skip]`, which needs the `mutants`
   crate as a dependency, and never a lowered threshold.

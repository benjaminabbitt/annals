# angzarr-embedded

angzarr-embedded is angzarr's **in-app runtime**: angzarr run inside your
application's own process, single-process and in memory or on SQLite, with
no servers to deploy. "Embedded" means embedded in your application, not
microcontroller firmware. For the distributed system (separate services,
brokers, mTLS between them), see angzarr core,
<https://github.com/angzarr-io/angzarr>; both share angzarr's wire contract.

It is a deterministic application engine on CQRS/ES: small Rust
crates, knowing nothing of the applications that use them. angzarr-embedded
depends on angzarr's wire types (angzarr-proto, prost, prost-types), which
its kernel operates on directly, on angzarr's shared store contract
(angzarr-ports) and on its localizable errors (angzarr-diagnostic), all
three angzarr core's shared crates; angzarr-store-sqlite depends on
angzarr-ports, sea-query and sqlx. Every error is localizable: a stable code
and named args (angzarr core's `angzarr-diagnostic`, re-exported as
`angzarr_embedded::angzarr_diagnostic`), with an en-US Fluent catalog per
crate under `crates/<crate>/locales/`. angzarr core also holds the strict
YAML reader with versioned documents (`angzarr-yaml`), which this
repository's crates do not use.

| Crate | What it is |
|---|---|
| [`angzarr-embedded`](crates/angzarr-embedded) | event-sourcing kernel on angzarr's proto types (`angzarr_embedded`): decide, emit, apply, replay; angzarr's local runtime |
| [`angzarr-embedded-host`](crates/angzarr-embedded-host) | remote components as child processes (the go-plugin model): launched from `[[component]]` config tables, which list every component, in-process ones too (`kind`, required, = `domain`, `policy`, `projector` or `upcaster`; read strictly: unknown keys and values refused, at their key path), handshaken, reached by gRPC on a Unix socket, supervised, and handed back to the kernel by kind (a domain as an angzarr-router `CommandHandler`, a policy as a `PolicyComponent`, a projector as a `ProjectorHandler`) |
| [`angzarr-store-sqlite`](crates/angzarr-store-sqlite) | angzarr-ports on SQLite, the one SQLite adapter angzarr-embedded and angzarr share: a leaf (nothing of angzarr-embedded), sea-query statements on sqlx, with its migrations; held to the ports' conformance suite |
| [`angzarr-embedded-macros`](crates/angzarr-embedded-macros) | markup over hand-written aggregates, process managers, policies and projectors, generating their `angzarr_embedded` glue |
| [`angzarr-embedded-catalog-check`](crates/angzarr-embedded-catalog-check) | test-only: every error code has a message, with exactly its args, in every shipped Fluent catalog |

```
just test           # cargo-nextest over the workspace
just lint           # clippy, and the licence lists
just lint-deps      # cargo-deny: licences, sources, bans, advisories (deny.toml)
just fmt            # cargo fmt over the workspace's crates (just fmt-check changes nothing)
just mutants        # cargo-mutants over the workspace; `just mutants angzarr-embedded` for one crate
just mutants-all    # the full pass: every mutant at HEAD, judged per crate (weekly in CI)
just wasm           # build angzarr-embedded for wasm32-unknown-unknown
just hooks-install  # install the git hooks (gates/)
```

angzarr-embedded depends on angzarr-proto, a git dependency whose build runs
`protoc`, so building angzarr-embedded (and every crate that depends on it, and so
`cargo build --workspace`) needs `protoc` on `PATH` and access to
github.com. The crates that do not depend on angzarr-embedded need nothing beyond
Rust. The recipes need [`just`](https://just.systems).
`just mutants` also needs
[`cargo-mutants`](https://mutants.rs) (`cargo install cargo-mutants --locked`);
its config is `.cargo/mutants.toml`.

## Licence

Licensed under either of Apache License 2.0 or BSD 3-Clause, at your option.

Some of angzarr-embedded's crates depend on `angzarr-proto`, angzarr's wire types,
which is AGPL-3.0-only. In a normal build (dev-dependencies, which only
tests build, aside):

- **Carry angzarr-proto:** `angzarr-embedded` and `angzarr-embedded-host`,
  which depend on it directly, and `angzarr-store-sqlite`, which depends on
  angzarr-ports (AGPL-3.0-only too). A binary that links any of these is effectively AGPL-3.0-only:
  their source stays Apache-2.0 OR BSD-3-Clause, but the linked whole carries
  angzarr-proto's terms.
- **Apache-2.0 OR BSD-3-Clause only:** `angzarr-embedded-catalog-check` and `angzarr-embedded-macros`.
  `angzarr-embedded-catalog-check` and `angzarr-embedded-macros` reach angzarr-proto only
  through dev-dependencies, so their tests link it but
  a binary built on them does not.

`just licence-check`, part of `just lint`, fails when these two lists stop
matching `cargo tree`.

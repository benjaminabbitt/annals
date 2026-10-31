//! Every error code angzarr-embedded's crates can produce has a message in
//! every shipped locale, using exactly the error's args; codes are unique;
//! and no variant goes unsampled.
//!
//! The errors come from each crate's samples, one value of every variant.
//! Each crate's catalogs, `crates/<crate>/locales/<locale>/<crate>.ftl`, are
//! checked against them by angzarr-diagnostic's [`catalog::check`]: a message
//! for every code with exactly its args, an error for every message, codes
//! under the crate's name, and a display of the code and args. A nested
//! cause of another crate's is that crate's to check (angzarr-ports' store
//! errors, which angzarr-embedded forwards under their own codes, are
//! checked against `angzarr_ports::EN_US` in core). A variant added without
//! a sample is caught by reading the crates' sources: every code literal
//! there (`"angzarr-embedded-..."`) must be the code of some sample. Each
//! crate's en-US catalog is also its exported `EN_US`, which must be that
//! file.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use angzarr_diagnostic::catalog::{self, Catalog};
use angzarr_diagnostic::{Arg, Diagnostic};

/// The crates whose errors are checked, and their samples.
fn samples() -> Vec<(&'static str, Vec<Box<dyn Diagnostic>>)> {
    vec![
        ("angzarr-embedded", angzarr_embedded::diagnostic_samples()),
        (
            "angzarr-embedded-host",
            angzarr_embedded_host::diagnostic_samples(),
        ),
        ("angzarr-store-sqlite", sqlite_samples()),
    ]
}

/// Each checked crate's exported en-US catalog: its name and its text.
fn exported() -> Vec<(&'static str, &'static str)> {
    vec![
        angzarr_embedded::EN_US,
        angzarr_embedded_host::EN_US,
        angzarr_store_sqlite::EN_US,
    ]
}

/// angzarr-store-sqlite's errors: each one it puts in a store error as its
/// cause, and a damage report.
fn sqlite_samples() -> Vec<Box<dyn Diagnostic>> {
    let mut samples: Vec<Box<dyn Diagnostic>> = angzarr_store_sqlite::errors()
        .into_iter()
        .map(|error| -> Box<dyn Diagnostic> { Box::new(error) })
        .collect();
    samples.push(Box::new(angzarr_store_sqlite::damage_sample()));
    samples
}

/// Crates that define no codes of their own.
const NO_CODES: &[&str] = &["angzarr-embedded-catalog-check", "angzarr-embedded-macros"];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Every crate's catalogs, by crate then locale: the text of each
/// `locales/<locale>/<crate>.ftl`. Any other file there is a problem.
fn catalogs(problems: &mut Vec<String>) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for krate in fs::read_dir(crates_dir()).unwrap() {
        let krate = krate.unwrap().path();
        let name = krate.file_name().unwrap().to_string_lossy().into_owned();
        let Ok(locales) = fs::read_dir(krate.join("locales")) else {
            continue;
        };
        let mut by_locale = BTreeMap::new();
        for locale in locales {
            let locale = locale.unwrap().path();
            let tag = locale.file_name().unwrap().to_string_lossy().into_owned();
            let file = locale.join(format!("{name}.ftl"));
            for other in fs::read_dir(&locale).unwrap() {
                let other = other.unwrap().path();
                if other != file {
                    problems.push(format!("{}: not {name}.ftl", other.display()));
                }
            }
            match fs::read_to_string(&file) {
                Ok(text) => {
                    by_locale.insert(tag, text);
                }
                Err(_) => problems.push(format!("{}: missing", file.display())),
            }
        }
        out.insert(name, by_locale);
    }
    out
}

/// Every code `d` produces that is `krate`'s: its own and, recursively, its
/// nested causes' under the crate's name.
fn own_codes(d: &dyn Diagnostic, krate: &str, out: &mut BTreeSet<String>) {
    out.insert(d.code().to_string());
    for (_, arg) in d.args() {
        if let Arg::Nested(inner) = arg {
            if inner.code().starts_with(&format!("{krate}-")) {
                own_codes(inner.as_ref(), krate, out);
            }
        }
    }
}

/// Every `"angzarr-embedded-..."` string literal in a crate's library source (not its
/// `*.test.rs` tests), outside comments: the codes its errors can produce.
fn code_literals(krate: &str) -> BTreeSet<String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs")
                && !p.to_string_lossy().ends_with(".test.rs")
            {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    walk(&crates_dir().join(krate).join("src"), &mut files);
    let prefix = format!("\"{krate}-");
    let mut codes = BTreeSet::new();
    for file in files {
        let text = fs::read_to_string(&file).unwrap();
        for line in text.lines().filter(|l| !l.trim_start().starts_with("//")) {
            let mut rest = line;
            while let Some(at) = rest.find(&prefix) {
                let body = &rest[at + 1..];
                let end = body.find('"').unwrap_or(body.len());
                let lit = &body[..end];
                if lit
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                {
                    codes.insert(lit.to_string());
                }
                rest = &body[end..];
            }
        }
    }
    codes
}

#[test]
fn every_code_has_a_message_with_exactly_its_args_in_every_locale() {
    let mut problems = Vec::new();
    let catalogs = catalogs(&mut problems);
    let locales: BTreeSet<String> = catalogs.values().flat_map(|c| c.keys().cloned()).collect();
    assert!(
        locales.contains("en-US"),
        "no en-US catalogs under {}",
        crates_dir().display()
    );

    let checked: Vec<&str> = samples().into_iter().map(|(krate, _)| krate).collect();
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for (krate, diagnostics) in samples() {
        if diagnostics.is_empty() {
            problems.push(format!("{krate}: no samples"));
        }
        // A store error the crate forwards under angzarr-ports' own code is
        // checked against angzarr-ports' catalog; every other sample, against
        // the crate's own.
        let (forwarded, own): (Vec<_>, Vec<_>) = diagnostics
            .into_iter()
            .partition(|d| d.code().starts_with("angzarr-ports-"));
        for locale in &locales {
            match catalogs.get(krate).and_then(|c| c.get(locale)) {
                None => problems.push(format!("{krate}: no {locale} catalog")),
                Some(ftl) => problems.extend(
                    catalog::check(krate, ftl, &own)
                        .into_iter()
                        .map(|p| format!("{krate} ({locale}): {p:?}")),
                ),
            }
        }
        // Every forwarded error has its message there; that each of
        // angzarr-ports' messages has an error is checked in core.
        problems.extend(
            catalog::check("angzarr-ports", angzarr_ports::EN_US.1, &forwarded)
                .into_iter()
                .filter(|p| !matches!(p, catalog::Problem::Unsampled { .. }))
                .map(|p| format!("{krate} forwarding angzarr-ports: {p:?}")),
        );
        let mut codes = BTreeSet::new();
        for d in &own {
            own_codes(&**d, krate, &mut codes);
        }
        for code in codes {
            if let Some(previous) = seen.insert(code.clone(), krate) {
                problems.push(format!("{code}: produced by {previous} and by {krate}"));
            }
        }
    }

    // Every crate with a catalog is checked, and every crate is either
    // checked or known to have no codes.
    for name in catalogs.keys() {
        if !checked.contains(&name.as_str()) {
            problems.push(format!("{name}: a catalog no samples are checked against"));
        }
    }
    let crates = fs::read_dir(crates_dir())
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.path().join("Cargo.toml").exists());
    for name in crates.map(|e| e.file_name().to_string_lossy().into_owned()) {
        if !checked.contains(&name.as_str()) && !NO_CODES.contains(&name.as_str()) {
            problems.push(format!(
                "{name}: a crate with neither samples nor a place in NO_CODES"
            ));
        }
    }

    // No variant without a sample: every code in a crate's source is seen.
    for krate in &checked {
        for code in code_literals(krate) {
            if !seen.contains_key(&code) {
                problems.push(format!("{code}: in {krate}'s source but in no sample"));
            }
        }
    }
    for krate in NO_CODES {
        for code in code_literals(krate) {
            problems.push(format!("{code}: {krate} is listed as having no codes"));
        }
    }

    problems.sort();
    problems.dedup();
    assert!(
        problems.is_empty(),
        "{} problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

/// A dependent renders from the exported catalog, so it must be the file
/// checked here, under the crate's own name, for every checked crate.
#[test]
fn every_checked_crate_exports_its_en_us_catalog() {
    let exported = exported();
    let names: Vec<&str> = exported.iter().map(|(name, _)| *name).collect();
    let checked: Vec<&str> = samples().iter().map(|(krate, _)| *krate).collect();
    assert_eq!(names, checked);
    for (name, text) in exported {
        let path = crates_dir()
            .join(name)
            .join("locales/en-US")
            .join(format!("{name}.ftl"));
        let file =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert_eq!(text, file, "{name}::EN_US is not {}", path.display());
    }
}

/// The store errors angzarr-embedded forwards under their own codes are
/// rendered from angzarr-ports' own catalog, re-exported here: no copy.
#[test]
fn angzarr_embedded_reexports_the_angzarr_ports_catalog() {
    let (name, text) = angzarr_embedded::angzarr_ports::EN_US;
    assert_eq!(name, "angzarr-ports");
    assert_eq!(text, angzarr_ports::EN_US.1);
}

/// The reasons angzarr-router refuses with: every `RouterReason` value but
/// proto3's zero, by its full name, read from the enum angzarr-embedded is
/// built with, so a reason the router adds is checked here without a list
/// to keep.
fn router_reasons() -> Vec<String> {
    let reasons: Vec<String> = angzarr_embedded::reasons::router()
        .map(|reason| reason.as_str_name().to_string())
        .collect();
    assert!(!reasons.is_empty(), "no reasons read from RouterReason");
    reasons
}

/// Every reason angzarr-router refuses with is an angzarr-embedded code of
/// its own, `angzarr-embedded-next-` and the reason without its
/// `ROUTER_REASON_` prefix in kebab-case, with a
/// message in every locale: `KernelError::Router` forwards the router's
/// `CodedError` under it.
#[test]
fn every_router_reason_has_its_own_code_and_message() {
    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::KernelError;

    let mut problems = Vec::new();
    let mine: BTreeMap<String, Catalog> = catalogs(&mut problems)["angzarr-embedded"]
        .iter()
        .map(|(locale, ftl)| (locale.clone(), Catalog::parse(ftl).expect("it parses")))
        .collect();
    for reason in router_reasons() {
        let expected = format!(
            "angzarr-embedded-next-{}",
            reason
                .strip_prefix("ROUTER_REASON_")
                .expect("a router reason carries its enum's prefix")
                .to_ascii_lowercase()
                .replace('_', "-")
        );
        let error = KernelError::Router(CodedError {
            code: reason.clone(),
            extras: Default::default(),
            grpc: angzarr_embedded::angzarr_router::error::GrpcCode::Internal,
            refusal: None,
        });
        if error.code() != expected {
            problems.push(format!("{reason} is {}, not {expected}", error.code()));
        }
        for (locale, catalog) in &mine {
            if catalog.variables(&expected).is_none() {
                problems.push(format!("{locale}: no message for {expected} ({reason})"));
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The crate a code belongs to: the longest name it starts with, of the
/// checked crates and angzarr-ports (whose store errors angzarr-embedded
/// forwards under their own codes).
fn owner(code: &str) -> Option<&'static str> {
    samples()
        .into_iter()
        .map(|(krate, _)| krate)
        .chain(["angzarr-ports"])
        .filter(|k| code.starts_with(&format!("{k}-")))
        .max_by_key(|k| k.len())
}

/// `code`'s message in `locale`, rendered with `args`: the text, and every
/// error Fluent reported (a variable the message names and `args` lack is
/// one). The catalog is the code's owner's ([`owner`]): a checked crate's
/// file, or angzarr-ports' own `EN_US`.
fn render(code: &str, args: &[(&str, Arg)], locale: &str) -> (String, Vec<String>) {
    use fluent_bundle::{FluentArgs, FluentBundle, FluentResource, FluentValue};
    let krate = owner(code).unwrap_or_else(|| panic!("{code}: no crate owns it"));
    let (path, text) = match krate {
        "angzarr-ports" if locale == "en-US" => (
            PathBuf::from("angzarr_ports::EN_US"),
            angzarr_ports::EN_US.1.to_string(),
        ),
        "angzarr-ports" => {
            return (
                String::new(),
                vec![format!("{code}: angzarr-ports ships no {locale} catalog")],
            )
        }
        _ => {
            let path = crates_dir()
                .join(krate)
                .join("locales")
                .join(locale)
                .join(format!("{krate}.ftl"));
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            (path, text)
        }
    };
    let resource = FluentResource::try_new(text).unwrap_or_else(|(r, _)| r);
    let mut bundle = FluentBundle::new(vec![locale.parse().expect("a locale tag")]);
    bundle.set_use_isolating(false);
    bundle
        .add_resource(resource)
        .unwrap_or_else(|e| panic!("{}: {e:?}", path.display()));
    let mut fluent = FluentArgs::new();
    for (name, arg) in args {
        let value = match arg {
            Arg::Int(n) => FluentValue::from(*n),
            Arg::Uint(n) => FluentValue::from(*n),
            Arg::Str(s) => FluentValue::from(s.clone()),
            Arg::Nested(cause) => FluentValue::from(cause.code().to_string()),
        };
        fluent.set(*name, value);
    }
    let Some(pattern) = bundle.get_message(code).and_then(|m| m.value()) else {
        return (
            String::new(),
            vec![format!("{code}: no message in {locale}")],
        );
    };
    let mut errors = Vec::new();
    let rendered = bundle.format_pattern(pattern, Some(&fluent), &mut errors);
    (
        rendered.into_owned(),
        errors.iter().map(|e| format!("{code}: {e:?}")).collect(),
    )
}

/// Every sample's message, in every shipped locale, renders with the
/// sample's own args and no error: no message names a variable its error
/// does not give, whichever args that error has.
#[test]
fn every_samples_message_renders_with_its_args() {
    let mut problems = Vec::new();
    let locales: BTreeSet<String> = catalogs(&mut problems)
        .values()
        .flat_map(|c| c.keys().cloned())
        .collect();
    fn each(d: &dyn Diagnostic, locales: &BTreeSet<String>, problems: &mut Vec<String>) {
        for locale in locales {
            let (_, errors) = render(d.code(), &d.args(), locale);
            problems.extend(errors.into_iter().map(|e| format!("{e} ({locale})")));
        }
        for (_, arg) in d.args() {
            if let Arg::Nested(inner) = arg {
                each(inner.as_ref(), locales, problems);
            }
        }
    }
    for (_, diagnostics) in samples() {
        for d in &diagnostics {
            each(&**d, &locales, &mut problems);
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// SQLite's failure renders whether or not the driver gave SQLite's result
/// code: with it, as the database's failure naming the code; without it
/// (an I/O error, a pool timing out), under a key of its own that names
/// none, so no `$errno` goes missing.
#[test]
fn sqlites_failure_renders_with_and_without_its_errno() {
    use angzarr_store_sqlite::SqlError;
    let detail = "database is locked".to_string();
    let rendered = |error: SqlError| render(error.code(), &Diagnostic::args(&error), "en-US");

    let with = rendered(SqlError::Unavailable {
        errno: Some(5),
        detail: detail.clone(),
    });
    assert_eq!(
        with,
        (
            "the database failed (SQLite code 5): database is locked".to_string(),
            vec![]
        )
    );

    let without = rendered(SqlError::Unavailable {
        errno: None,
        detail,
    });
    assert_eq!(
        without,
        (
            "the database driver failed: database is locked".to_string(),
            vec![]
        )
    );
}

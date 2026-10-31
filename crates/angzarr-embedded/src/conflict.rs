//! Conflict units: what COMMUTATIVE compares, exactly as angzarr defines
//! them (`options.proto`, the `conflict_units` region).
//!
//! A state message lists COMMUTATIVE in its `(io.angzarr.v1.state)` option
//! and marks its fields with `(io.angzarr.v1.conflict)`. From those markers
//! angzarr's generator (`angzarr_build::compile` for Rust, backlog CR-1)
//! emits, per state message, a [`State`](super::State) impl: the
//! `StateDescriptor` it registers with, and the pure function
//!
//! ```text
//! changed(before, after) -> the unit paths whose values differ
//! ```
//!
//! Business code never writes it: the generated code shares angzarr's
//! semantics table and is held to the same golden parity suite as every
//! client library, so a state resolves identically under both runtimes.
//! The only hand-written implementations are test fixtures standing in for
//! generated code, until the generator exists.
//!
//! This module is the generated code's interface: the path type it builds
//! ([`UnitPath`], with [`Key`]) and the set it returns ([`Units`]).
//! angzarr-router resolves COMMUTATIVE (`DomainDispatch::with_commutative`)
//! and reports both sets' paths as text; the kernel judges them by
//! options.proto's overlap rule ([`judge`], decision 14.42).
//!
//! **The risk.** decide runs on the head, so the invariants it checks still
//! hold; what no structural comparison sees is a value derived from, or
//! meaningful only together with, another unit (a total over items, two
//! fields that must change together). That is a modelling failure, fixed
//! by putting dependent data in one unit (mark the containing message
//! without `descend`), deriving the value in decide or a projection rather
//! than storing it, or sending that command STRICT. angzarr's
//! `docs/guide/state-and-conflicts.md` shows the failures and the fixes.

use std::collections::BTreeSet;
use std::fmt;

use angzarr_proto::v1::ConflictResolution;
use angzarr_router::pb::ConflictReport;

/// One conflict unit's path (`options.proto`, *Unit path syntax*): proto
/// field names (never JSON names) joined by `.`, a map or keyed-repeated
/// key in brackets after its field (`items[42]`, `items[42].weight`), the
/// order unit `field[#order]`, and the **wildcard**: `*` followed by the
/// path of an unmarked field that changed (`*notes`), or `*` alone for
/// unknown fields at the top level.
///
/// Held as bytes, because a bytes key is written raw; compared as byte
/// strings, as the proto says. Built only through the constructors, so the
/// escaping is the kernel's, once.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct UnitPath(Vec<u8>);

/// A map or element key, as a path writes it: integers in decimal, bools
/// as `true` / `false`, enums (element keys only) by number, strings as
/// their UTF-8 and bytes raw, each `\`, `]` and `#` in a string or bytes
/// key preceded by a `\` (so the string key `"#order"` is `[\#order]` and
/// never collides with the order unit).
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Key<'a> {
    Int(i64),
    Uint(u64),
    Bool(bool),
    Enum(i32),
    Str(&'a str),
    Bytes(&'a [u8]),
}

impl UnitPath {
    /// The top-level field `name`.
    pub fn field(name: &str) -> UnitPath {
        UnitPath(name.as_bytes().to_vec())
    }

    /// The inner field `name` of this (descended) message: `self.name`.
    pub fn dot(&self, name: &str) -> UnitPath {
        let mut path = self.0.clone();
        path.push(b'.');
        path.extend_from_slice(name.as_bytes());
        UnitPath(path)
    }

    /// This map's or keyed list's entry `key`: `self[key]`, escaped.
    pub fn key(&self, key: Key<'_>) -> UnitPath {
        let mut path = self.0.clone();
        path.push(b'[');
        let escaped = |path: &mut Vec<u8>, raw: &[u8]| {
            for &b in raw {
                if matches!(b, b'\\' | b']' | b'#') {
                    path.push(b'\\');
                }
                path.push(b);
            }
        };
        match key {
            Key::Int(n) => path.extend_from_slice(n.to_string().as_bytes()),
            Key::Uint(n) => path.extend_from_slice(n.to_string().as_bytes()),
            Key::Bool(b) => path.extend_from_slice(if b { b"true" } else { b"false" }),
            Key::Enum(n) => path.extend_from_slice(n.to_string().as_bytes()),
            Key::Str(s) => escaped(&mut path, s.as_bytes()),
            Key::Bytes(raw) => escaped(&mut path, raw),
        }
        path.push(b']');
        UnitPath(path)
    }

    /// This keyed, ordered list's order unit: `self[#order]`.
    pub fn order(&self) -> UnitPath {
        let mut path = self.0.clone();
        path.extend_from_slice(b"[#order]");
        UnitPath(path)
    }

    /// The wildcard for this unmarked field: `*` then its path. Overlaps
    /// every unit of a non-empty other set.
    pub fn wildcard(&self) -> UnitPath {
        let mut path = Vec::with_capacity(self.0.len() + 1);
        path.push(b'*');
        path.extend_from_slice(&self.0);
        UnitPath(path)
    }

    /// The wildcard for unknown fields at the top level: `*` alone.
    pub fn unknown() -> UnitPath {
        UnitPath(b"*".to_vec())
    }

    pub fn is_wildcard(&self) -> bool {
        self.0.first() == Some(&b'*')
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// For messages and logs: the path as text, any bytes key shown lossily.
impl fmt::Display for UnitPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.0))
    }
}

/// The units one change touched: what [`State::changed`](super::State::changed)
/// returns. Ordered, so the kernel stays deterministic.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Units(BTreeSet<UnitPath>);

impl Units {
    pub fn new() -> Units {
        Units::default()
    }

    pub fn insert(&mut self, unit: UnitPath) {
        self.0.insert(unit);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &UnitPath> {
        self.0.iter()
    }
}

impl Units {
    /// The set a state with no conflict units changes on any change: the
    /// top-level wildcard alone, which overlaps every non-empty set. The
    /// default [`State::changed`](super::State::changed).
    pub fn everything() -> Units {
        Units::from_iter([UnitPath::unknown()])
    }
}

impl FromIterator<UnitPath> for Units {
    fn from_iter<I: IntoIterator<Item = UnitPath>>(iter: I) -> Units {
        Units(iter.into_iter().collect())
    }
}

/// The kernel's judgement of a router's conflict report (decision 14.42).
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum Judgement {
    /// The events may be kept.
    Kept,
    /// AGGREGATE_HANDLES: the handler did not hand back the nonce
    /// delivered.
    Unresolved,
    /// COMMUTATIVE: the window's units and the command's overlap.
    Overlap {
        window: Vec<String>,
        command: Vec<String>,
    },
}

/// A command's conflicts, judged from the router's `report` (none counts
/// as empty) under the `resolution` it was delivered under with `nonce`:
/// under AGGREGATE_HANDLES the events are kept only if the reported nonce
/// is the one delivered; under COMMUTATIVE only if the window's units and
/// the command's do not [`overlap`]; under STRICT always.
pub(crate) fn judge(
    resolution: ConflictResolution,
    nonce: &[u8],
    report: Option<&ConflictReport>,
) -> Judgement {
    let empty = ConflictReport::default();
    let report = report.unwrap_or(&empty);
    match resolution {
        ConflictResolution::AggregateHandles if report.resolved_nonce[..] != *nonce => {
            Judgement::Unresolved
        }
        ConflictResolution::Commutative if overlap(&report.window_units, &report.command_units) => {
            Judgement::Overlap {
                window: report.window_units.clone(),
                command: report.command_units.clone(),
            }
        }
        _ => Judgement::Kept,
    }
}

/// Whether two unit sets, as paths, overlap (`options.proto`, *Overlap*):
/// neither is empty, and one holds a wildcard, or a unit of one equals a
/// unit of the other or is a path prefix of it ending at a `.` or `[`
/// boundary.
pub(crate) fn overlap(a: &[String], b: &[String]) -> bool {
    if a.is_empty() || b.is_empty() {
        return false;
    }
    if a.iter().chain(b).any(|unit| unit.starts_with('*')) {
        return true;
    }
    let encloses = |prefix: &str, unit: &str| {
        unit.strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with(['.', '[']))
    };
    a.iter()
        .any(|x| b.iter().any(|y| x == y || encloses(x, y) || encloses(y, x)))
}

#[cfg(test)]
#[path = "conflict.test.rs"]
mod tests;

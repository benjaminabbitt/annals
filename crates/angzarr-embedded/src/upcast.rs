//! Upcasting by type URL (decision 14.39): the kernel's own, the router
//! has none. Every payload the kernel hands on is upcast first, through
//! the domain's [`UpcastPort`]: its upcaster sidecar, or an in-process
//! [`Upcaster`], called directly under the same contract.
//!
//! **The chain.** Rules are kept in registration order. Every rule whose
//! source type matches the payload's *current* type applies, and its output
//! is the next rule's input, so V1 → V2 → V3 composes; the chain stops when
//! no later rule matches. Other types pass through unchanged. A rule's
//! source is a fully-qualified type name (`pkg.v1.Foo`), compared with the
//! type URL's name ([`type_name`](super::type_name)).
//!
//! **Events and commands, one chain.** The same rules run on a stored
//! event's payload at read time and on a command's (or fact's) payload
//! before it is decoded. Upcasting commands is **discouraged**: it hides
//! stale clients, which keep working so nobody upgrades them, and it is
//! machinery for data that is never stored. Prefer evolving a command
//! compatibly (add fields), or decoding both versions in the domain for a
//! transition; upcast a command only as a migration aid with a planned
//! removal. Event upcasters are permanent; stored events never go away.
//!
//! **Strict.** The chain passes an unknown type through and leaves the
//! consequence to whoever decodes, and the decode is strict: after
//! upcasting, a type the domain does not decode is refused naming its type
//! URL ([`KernelError::UnknownType`](super::KernelError::UnknownType) for a
//! command, [`KernelError::Unreadable`](super::KernelError::Unreadable) for
//! an event), never skipped. Typed rules ([`Upcaster::rule`]) record their
//! target type too, so [`Registry::check`](super::Registry::check) can
//! refuse a chain that cannot finish (a duplicate source, rules out of
//! order, a dead end) before anything is read.

use angzarr_diagnostic::Report;
use angzarr_proto::v1::{EventPage, UpcastRequest, UpcastResponse};
use angzarr_router::send::BoxFuture;
use prost::{Message, Name};
use prost_types::Any;

use super::error::{ChainProblem, PayloadError, UpcastChainError, UpcastError};
use super::payload::{pack, type_name};

/// Transforms one payload to its next schema version.
pub type UpcastFn = Box<dyn Fn(&Any) -> Result<Any, UpcastError> + Send + Sync>;

/// What the kernel upcasts an incoming payload through (decision 14.39):
/// its component's upcaster sidecar, `UpcasterService.Upcast`'s body. An
/// in-process [`Upcaster`] is one, called directly under the same
/// contract, answering at once; angzarr-embedded-host's
/// `kind = "upcaster"` child is another, whose answer the kernel awaits.
/// Object-safe: the future is boxed ([`BoxFuture`]), `Send` on every target
/// but `wasm32`, as the router's dispatch twins' are.
pub trait UpcastPort: Send + Sync {
    /// Every page of `request` through the chain, for `request.domain`.
    fn upcast<'a>(
        &'a self,
        request: &'a UpcastRequest,
    ) -> BoxFuture<'a, Result<UpcastResponse, UpcastError>>;
}

impl UpcastPort for Upcaster {
    fn upcast<'a>(
        &'a self,
        request: &'a UpcastRequest,
    ) -> BoxFuture<'a, Result<UpcastResponse, UpcastError>> {
        Box::pin(std::future::ready(self.dispatch(request)))
    }
}

/// `payload`, an incoming command's or fact's, upcast through `port` for
/// `domain` (decision 14.39): sent as a one-page request whose page carries
/// only the payload, the payload of the page returned taken. A port that
/// answers no page leaves it as sent.
pub(crate) async fn upcast_input(
    port: &dyn UpcastPort,
    domain: &str,
    payload: &Any,
) -> Result<Any, UpcastError> {
    let request = UpcastRequest {
        domain: domain.to_string(),
        events: vec![EventPage {
            payload: Some(payload.clone()),
            ..EventPage::default()
        }],
    };
    Ok(port
        .upcast(&request)
        .await?
        .events
        .into_iter()
        .next()
        .and_then(|page| page.payload)
        .unwrap_or_else(|| payload.clone()))
}

/// `pages` of a `domain` stream, read from the store, upcast through
/// `port` (decision 14.39): sent as one request, the pages it answers
/// taken in their place. No pages, no request.
pub(crate) async fn upcast_pages(
    port: &dyn UpcastPort,
    domain: &str,
    pages: &[EventPage],
) -> Result<Vec<EventPage>, UpcastError> {
    if pages.is_empty() {
        return Ok(Vec::new());
    }
    let request = UpcastRequest {
        domain: domain.to_string(),
        events: pages.to_vec(),
    };
    Ok(port.upcast(&request).await?.events)
}

/// One rule: its source type's full name, its target's when known (typed
/// rules), and the transform.
struct Rule {
    source: String,
    target: Option<String>,
    transform: UpcastFn,
}

/// The rule table for one domain.
pub struct Upcaster {
    name: String,
    domain: String,
    rules: Vec<Rule>,
}

impl Upcaster {
    /// An empty upcaster named `name`, for `domain`: what a domain with
    /// nothing to upcast registers.
    pub fn new(name: impl Into<String>, domain: impl Into<String>) -> Self {
        Upcaster {
            name: name.into(),
            domain: domain.into(),
            rules: Vec::new(),
        }
    }

    /// Registers a rule transforming payloads of the fully-qualified
    /// `source_type`. Its target is not known, so
    /// [`Registry::check`](super::Registry::check) cannot follow it.
    pub fn on_event(
        mut self,
        source_type: &str,
        thunk: impl Fn(&Any) -> Result<Any, UpcastError> + Send + Sync + 'static,
    ) -> Self {
        self.rules.push(Rule {
            source: source_type.to_string(),
            target: None,
            transform: Box::new(thunk),
        });
        self
    }

    /// Registers a typed rule from `From` to `To`: unpack `From`, run
    /// `step`, pack `To` ([`pack`](super::pack)). A step that cannot convert
    /// returns the domain's own error as a report.
    pub fn rule<From, To>(self, step: fn(From) -> Result<To, Report>) -> Self
    where
        From: Message + Name + Default + 'static,
        To: Message + Name + 'static,
    {
        let source = From::full_name();
        let transform = {
            let source = source.clone();
            move |any: &Any| {
                let from = From::decode(&any.value[..]).map_err(|error| {
                    UpcastError::Payload(PayloadError::Decode {
                        type_url: any.type_url.clone(),
                        error,
                    })
                })?;
                let to = step(from).map_err(|error| UpcastError::Rule {
                    source: source.clone(),
                    error,
                })?;
                Ok(pack(&to))
            }
        };
        let mut upcaster = self;
        upcaster.rules.push(Rule {
            source,
            target: Some(To::full_name()),
            transform: Box::new(transform),
        });
        upcaster
    }

    /// The component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The domain whose payloads this upcaster transforms.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// The registered source types, in registration order.
    pub fn source_types(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.source.as_str()).collect()
    }

    /// Runs one payload (a stored event's, or a command's or fact's) through
    /// the rule chain.
    pub fn upcast_event(&self, event: &Any) -> Result<Any, UpcastError> {
        let mut current = event.clone();
        for rule in &self.rules {
            if type_name(&current.type_url) == rule.source {
                current = (rule.transform)(&current)?;
            }
        }
        Ok(current)
    }

    /// One page with its payload (if any) run through the rule chain; its
    /// `sequence` and `meta` kept.
    pub fn upcast_page(&self, page: &EventPage) -> Result<EventPage, UpcastError> {
        Ok(EventPage {
            payload: page
                .payload
                .as_ref()
                .map(|p| self.upcast_event(p))
                .transpose()?,
            ..page.clone()
        })
    }

    /// `UpcasterService.Upcast`'s body: every page's event through the rule
    /// chain; a request for another domain unchanged.
    pub fn dispatch(&self, request: &UpcastRequest) -> Result<UpcastResponse, UpcastError> {
        let events = if request.domain == self.domain {
            request
                .events
                .iter()
                .map(|p| self.upcast_page(p))
                .collect::<Result<_, _>>()?
        } else {
            request.events.clone()
        };
        Ok(UpcastResponse { events })
    }

    /// Check the typed rules can all run, given the type URLs the domain
    /// decodes: no two from one source, none registered before the rule
    /// producing its source, and every chain ending at a decodable type.
    /// Untyped rules ([`Upcaster::on_event`]) are not followed. Every
    /// problem is listed.
    pub fn check(&self, current: &[String]) -> Result<(), Vec<UpcastChainError>> {
        let problem = |problem| UpcastChainError {
            domain: self.domain.clone(),
            problem,
        };
        let typed: Vec<(usize, &str, &str)> = (self.rules.iter().enumerate())
            .filter_map(|(i, r)| Some((i, r.source.as_str(), r.target.as_deref()?)))
            .collect();
        let mut problems = Vec::new();
        for (n, &(i, source, _)) in typed.iter().enumerate() {
            if typed[..n].iter().any(|&(_, s, _)| s == source) {
                problems.push(problem(ChainProblem::Duplicate {
                    source: source.to_string(),
                }));
            }
            if let Some(&(_, produced_by, _)) =
                typed.iter().find(|&&(j, _, t)| j > i && t == source)
            {
                problems.push(problem(ChainProblem::OutOfOrder {
                    source: source.to_string(),
                    produced_by: produced_by.to_string(),
                }));
            }
        }
        // Each chain from its head (a source no typed rule produces), as a
        // payload of that type runs through the rules.
        let mut heads: Vec<&str> = Vec::new();
        for &(_, source, _) in &typed {
            if typed.iter().any(|&(_, _, t)| t == source) || heads.contains(&source) {
                continue;
            }
            heads.push(source);
            let mut at = source;
            let mut followed = true;
            for rule in &self.rules {
                if rule.source != at {
                    continue;
                }
                match &rule.target {
                    Some(target) => at = target,
                    None => {
                        followed = false;
                        break;
                    }
                }
            }
            if followed && !current.iter().any(|url| type_name(url) == at) {
                problems.push(problem(ChainProblem::DeadEnd {
                    source: source.to_string(),
                    ends_at: at.to_string(),
                }));
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }
}

#[cfg(test)]
#[path = "upcast.test.rs"]
mod tests;

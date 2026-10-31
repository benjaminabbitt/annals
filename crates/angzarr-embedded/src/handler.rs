//! Reacting to committed events: one callback shape for projections,
//! policies and the application's own reactions.

use angzarr_proto::v1::{Cover, EventRef};

use super::domain::Domain;
use super::stream::Envelope;

/// One committed event as a handler sees it: which stream, where in it, its
/// metadata and payload, and the stream's state just after it.
pub struct Committed<'a, D: Domain> {
    pub cover: &'a Cover,
    pub envelope: &'a Envelope<D::Event>,
    pub state: &'a D::State,
}

impl<'a, D: Domain> Committed<'a, D> {
    /// The event's reference: its stream and sequence (angzarr's
    /// `EventRef`), what a policy's command names as its causation (on the
    /// command only; the command's events name the command).
    pub fn event_ref(&self) -> EventRef {
        EventRef {
            stream: Some(self.cover.clone()),
            sequence: self.envelope.sequence(),
        }
    }

    pub fn cover(&self) -> &'a Cover {
        self.cover
    }

    pub fn sequence(&self) -> u64 {
        self.envelope.sequence()
    }

    pub fn envelope(&self) -> &'a Envelope<D::Event> {
        self.envelope
    }

    pub fn event(&self) -> &'a D::Event {
        self.envelope.event()
    }
}

/// Anything that reacts to a domain's committed events: a projection, a
/// policy's [`PolicyHandler`](super::PolicyHandler), an application callback. Called
/// after commit, in log order, once per event. It cannot change the stream
/// it is reading; it can only record, or hand off commands for others.
///
/// Any `FnMut(&Committed<D>)` is a handler.
///
/// Delivery to an in-process handler is exactly once per commit.
pub trait Handler<D: Domain> {
    fn handle(&mut self, committed: &Committed<'_, D>);
}

impl<D: Domain, F: FnMut(&Committed<'_, D>)> Handler<D> for F {
    fn handle(&mut self, committed: &Committed<'_, D>) {
        self(committed)
    }
}

#[cfg(test)]
#[path = "handler.test.rs"]
mod tests;

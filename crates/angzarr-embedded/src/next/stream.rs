//! A stream: an append-only log of stamped events and the state folded from
//! it.

use angzarr_diagnostic::Report;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{CommandBook, Cover, EventMeta, EventPage, FactOrigin, Snapshot, Uuid};
use prost::{Message, Name};
use prost_types::Timestamp;

use super::check::{check, Malformed, Sampled};
use super::coordinator::{
    admit, dispatch, expect, resolution, unknown_domain, upcast, Admitted, Contextual,
};
use super::dead_letter::DeadLetterLog;
use super::domain::{Domain, State};
use super::error::{unreadable, Failure, KernelError, PayloadError, ReplayError};
use super::handler::{Committed, Handler};
use super::payload::{pack, type_name, Payload};
use super::registry::Registry;
use super::route::Rejected;
use super::upcast::Upcaster;

/// One recorded event, checked and decoded: an [`EventPage`] whose
/// `EventMeta` had every field a recorded event always has (so its
/// accessors are not optional) and whose payload, upcast, decoded as `E`.
/// Built only by the kernel: by stamping a decision, or by
/// [`Envelope::decode`].
#[derive(Clone, PartialEq, Debug)]
pub struct Envelope<E> {
    pub(crate) sequence: u64,
    pub(crate) causation: Causation,
    pub(crate) correlation_id: Uuid,
    pub(crate) fact: Option<FactOrigin>,
    pub(crate) recorded_at: Timestamp,
    pub(crate) rules_version: String,
    pub(crate) event: E,
}

impl<E> Envelope<E> {
    /// `EventPage.sequence`: always definite.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Its direct cause (angzarr's `EventMeta.causation`): the producing
    /// command's id. Never absent on a recorded event.
    ///
    /// For an event of a policy's command this is the command, never the
    /// policy's trigger: events record their direct cause only (angzarr
    /// decision 13.3). The trigger is the command's own causation
    /// ([`Identified::causation_id`](super::Identified::causation_id)),
    /// which goes to tracing, not to the event.
    pub fn causation(&self) -> &Causation {
        &self.causation
    }

    /// The producing command's id, what deduplication finds: a command's,
    /// a fact's derived id, or a reaction's.
    pub fn command_id(&self) -> &Uuid {
        let Causation::CommandId(id) = &self.causation;
        id
    }

    /// Never absent on a recorded event.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// The source, for a fact's event.
    pub fn fact(&self) -> Option<&FactOrigin> {
        self.fact.as_ref()
    }

    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// The full version of the build that decided the event
    /// (`EventMeta.rules_version`): it explains the decision, and is never
    /// read to fold it.
    pub fn rules_version(&self) -> &str {
        &self.rules_version
    }

    pub fn event(&self) -> &E {
        &self.event
    }

    /// The metadata, as angzarr's `EventMeta`.
    pub fn meta(&self) -> EventMeta {
        EventMeta {
            causation: Some(self.causation.clone()),
            fact: self.fact.clone(),
            correlation_id: Some(self.correlation_id.clone()),
            recorded_at: Some(self.recorded_at),
            rules_version: self.rules_version.clone(),
        }
    }

    /// As an `EventPage { sequence, meta, payload }`: what the store
    /// appends and an angzarr reader is served.
    pub fn page(&self) -> EventPage
    where
        E: Payload,
    {
        EventPage {
            payload: Some(self.event.to_any()),
            meta: Some(self.meta()),
            sequence: self.sequence,
        }
    }

    /// A stored page of `stream`, read back: its meta checked complete
    /// ([`ReplayError::IncompleteMeta`](super::ReplayError::IncompleteMeta) otherwise), its payload run through
    /// `upcaster` and decoded as `E`. A type nothing upcasts or decodes is
    /// [`KernelError::Unreadable`] naming its type URL: replay is strict.
    pub fn decode(
        stream: &Cover,
        page: &EventPage,
        upcaster: &Upcaster,
    ) -> Result<Envelope<E>, KernelError>
    where
        E: Payload,
    {
        let sequence = page.sequence;
        let meta = Envelope::stamped(sequence, page.meta.clone().unwrap_or_default(), ())
            .map_err(KernelError::Replay)?;
        let unreadable = |reason: Report| unreadable(stream, sequence, reason);
        let payload = upcaster
            .upcast_event(&page.payload.clone().unwrap_or_default())
            .map_err(|e| unreadable(Report::of(&e)))?;
        let event = E::from_any(&payload).map_err(|e| unreadable(Report::of(&e)))?;
        Ok(meta.with(event))
    }

    /// Event `sequence` of `event` with `meta`, its required fields checked
    /// present and well-formed: how the kernel stamps a decision's events
    /// ([`Identified::meta`](super::Identified::meta)) and reads a stored
    /// page's.
    pub(crate) fn stamped(
        sequence: u64,
        meta: EventMeta,
        event: E,
    ) -> Result<Envelope<E>, ReplayError> {
        let incomplete = |field| ReplayError::IncompleteMeta { sequence, field };
        let causation = match meta.causation {
            Some(Causation::CommandId(id)) if id.is_well_formed() => Causation::CommandId(id),
            _ => return Err(incomplete("causation")),
        };
        let correlation_id = meta
            .correlation_id
            .filter(Uuid::is_well_formed)
            .ok_or_else(|| incomplete("correlation_id"))?;
        let recorded_at = meta.recorded_at.ok_or_else(|| incomplete("recorded_at"))?;
        Ok(Envelope {
            sequence,
            causation,
            correlation_id,
            fact: meta.fact,
            recorded_at,
            rules_version: meta.rules_version,
            event,
        })
    }

    /// The same metadata, holding `event`.
    fn with<F>(self, event: F) -> Envelope<F> {
        Envelope {
            sequence: self.sequence,
            causation: self.causation,
            correlation_id: self.correlation_id,
            fact: self.fact,
            recorded_at: self.recorded_at,
            rules_version: self.rules_version,
            event,
        }
    }
}

/// What [`Stream::execute`] did.
#[derive(Debug, PartialEq)]
pub enum Executed<'a, E> {
    /// Business code ran; these are the events it committed (possibly
    /// none).
    Applied(&'a [Envelope<E>]),
    /// The stream already held this command's events: these. Nothing ran.
    Duplicate(&'a [Envelope<E>]),
}

/// A domain's event log and the state folded from it, under one cover
/// whose domain is `D::NAME`. Its state changes only as events are committed
/// to it ([`Stream::execute`]).
///
/// A stream resumed from a snapshot ([`Stream::resume`]) holds only the
/// events after it: [`Stream::events`] starts at [`Stream::first`], and
/// [`Stream::len`] counts from 0 all the same.
pub struct Stream<D: Domain> {
    cover: Cover,
    /// The sequence of the first held event: 0, or the snapshot's + 1.
    first: u64,
    /// The state before the first held event: the default, or the
    /// snapshot's.
    base: D::State,
    events: Vec<Envelope<D::Event>>,
    state: D::State,
    /// The last event's `recorded_at`: the floor for the next.
    last_recorded_at: Option<Timestamp>,
}

impl<D: Domain> Stream<D> {
    /// An empty `D` stream: `root` in `edition` (`None`: main). Refuses a
    /// root that is not 16 bytes and a set empty edition, as [`check`](super::check)
    /// would.
    pub fn new(root: Uuid, edition: Option<String>) -> Result<Stream<D>, Malformed> {
        if !root.is_well_formed() {
            return Err(Malformed::BadRoot {
                len: root.value.len(),
            });
        }
        if edition.as_deref() == Some("") {
            return Err(Malformed::EmptyEdition);
        }
        Ok(Stream::empty(Cover {
            domain: D::NAME.to_string(),
            root: Some(root),
            edition,
        }))
    }

    fn empty(cover: Cover) -> Stream<D> {
        Stream::from_state(cover, D::State::default())
    }

    /// An empty stream whose state before any event is `state`.
    fn from_state(cover: Cover, state: D::State) -> Stream<D> {
        Stream {
            cover,
            first: 0,
            base: state.clone(),
            events: Vec::new(),
            state,
            last_recorded_at: None,
        }
    }

    /// `D`'s upcaster. Without it nothing of `D` decodes.
    fn typed(registry: &Registry) -> Result<&Upcaster, KernelError> {
        registry
            .upcaster(D::NAME)
            .ok_or_else(|| KernelError::UnregisteredDomain {
                domain: D::NAME.to_string(),
            })
    }

    fn check_domain(cover: &Cover) -> Result<(), KernelError> {
        if cover.domain == D::NAME {
            Ok(())
        } else {
            Err(KernelError::Replay(ReplayError::WrongDomain {
                found: cover.domain.clone(),
                expected: D::NAME,
            }))
        }
    }

    /// Fold stored `pages` onto the end of the stream, each checked as
    /// [`Stream::replay`] says.
    fn fold(&mut self, pages: &[EventPage], registry: &Registry) -> Result<(), KernelError> {
        let upcaster = Self::typed(registry)?;
        for (index, page) in pages.iter().enumerate() {
            if page.sequence != self.len() {
                return Err(KernelError::Replay(ReplayError::OutOfSequence {
                    index,
                    found: page.sequence,
                }));
            }
            let envelope = Envelope::decode(&self.cover, page, upcaster)?;
            let sequence = envelope.sequence;
            if let Some(last) = self.last_recorded_at {
                let at = envelope.recorded_at;
                if (at.seconds, at.nanos) < (last.seconds, last.nanos) {
                    return Err(KernelError::Replay(ReplayError::RecordedBackwards {
                        sequence,
                    }));
                }
            }
            self.push(envelope);
        }
        Ok(())
    }

    /// Fold one event in and hold it.
    fn push(&mut self, envelope: Envelope<D::Event>) {
        D::apply(&mut self.state, &envelope.event);
        self.last_recorded_at = Some(envelope.recorded_at);
        self.events.push(envelope);
    }

    /// The whole pipeline on this stream, in memory, with no store: for
    /// tests and in-memory use ([`Kernel::handle`](super::Kernel::handle)
    /// is the full loop, with a store and a chain). [`check`](super::check),
    /// identify, admit it against this stream's own events (deduplication,
    /// a process manager's start rule), resolve the
    /// handling, dispatch it through the router (awaited), commit, then
    /// hand each new event to `handlers`. A book for another stream is
    /// [`KernelError::WrongDomain`] or [`KernelError::Stale`].
    pub async fn execute(
        &mut self,
        book: CommandBook,
        registry: &Registry,
        sampled: &Sampled,
        dead_letters: &dyn DeadLetterLog,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<Executed<'_, D::Event>, Failure<Rejected>> {
        let kernel = Failure::Kernel;
        let checked =
            check(book, registry).map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        if checked.domain() != D::NAME {
            return Err(kernel(KernelError::WrongDomain {
                stream: checked.cover().clone(),
                expected: D::NAME,
            }));
        }
        let command = checked.identify(sampled);
        let head = self.len();
        if command.cover() != &self.cover {
            return Err(kernel(KernelError::Stale {
                stream: command.cover().clone(),
                expected: command.expected_sequence().unwrap_or(head),
                actual: head,
            }));
        }
        registry
            .handler(D::NAME)
            .ok_or_else(|| kernel(unknown_domain(D::NAME)))?;
        let found = self.command_run(command.command_id());
        let recorded: Vec<EventPage> = self.events[found.clone()]
            .iter()
            .map(Envelope::page)
            .collect();
        let current = upcast(registry, &command).await;
        match admit(registry, &command, &current, recorded, head > 0)? {
            Admitted::Duplicate(_) => return Ok(Executed::Duplicate(&self.events[found])),
            Admitted::Skip => return Ok(Executed::Applied(&[])),
            Admitted::Dispatch => {}
        }
        let resolution = resolution(registry, &command, &current).map_err(kernel)?;
        let mut nonces = head;
        let (expected, nonce) = expect(&command, resolution, head, &mut nonces).map_err(kernel)?;
        if nonce.is_some() && expected < self.first {
            // The window starts inside the snapshot: its events are not held.
            return Err(kernel(KernelError::BeforeSnapshot {
                stream: self.cover.clone(),
                first: self.first,
            }));
        }
        let snapshot = (self.first > 0).then(|| Snapshot {
            sequence: self.first - 1,
            state: Some(pack(&self.base.to_snapshot())),
            rules_version: registry.rules_version(D::NAME).unwrap_or_default(),
        });
        let current = current.map_err(|error| {
            kernel(KernelError::Upcast {
                stream: command.cover().clone(),
                error,
            })
        })?;
        let contextual = Contextual {
            command,
            current,
            resolution,
            expected,
            snapshot,
            pages: self.events.iter().map(Envelope::page).collect(),
            head,
            last_recorded_at: self.last_recorded_at,
            nonce,
        };
        let pages = dispatch(&contextual, sampled, registry, dead_letters).await?;
        let from = self.events.len();
        self.commit(&pages, registry, handlers).map_err(kernel)?;
        Ok(Executed::Applied(&self.events[from..]))
    }

    /// Fold `pages`, stamped events continuing the stream, into it, then
    /// hand each to every handler in order, with the state just after it.
    /// Returns the new length.
    pub(crate) fn commit(
        &mut self,
        pages: &[EventPage],
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<u64, KernelError> {
        for page in pages {
            self.fold(std::slice::from_ref(page), registry)?;
            let committed = Committed {
                cover: &self.cover,
                envelope: &self.events[self.events.len() - 1],
                state: &self.state,
            };
            for handler in handlers.iter_mut() {
                handler.handle(&committed);
            }
        }
        Ok(self.len())
    }

    /// Fold `pages` onto the end of the stream: a chain's step.
    pub(crate) fn extend(
        &mut self,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<(), KernelError> {
        self.fold(pages, registry)
    }

    /// Rebuild a stream from its whole stored log, from a fresh state (the
    /// registry's factory for `D`, if it holds one, else the default), each
    /// page decoded
    /// ([`Envelope::decode`], upcast) and folded by `D::apply`. Refuses a
    /// cover of another domain, a sequence out of order, a triggering event in
    /// this stream that is not earlier, and a `recorded_at` earlier than the
    /// event before.
    pub fn replay(
        cover: Cover,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        Self::check_domain(&cover)?;
        let mut stream = Stream::from_state(cover, registry.fresh::<D>());
        stream.fold(pages, registry)?;
        Ok(stream)
    }

    /// Rebuild a stream from a snapshot and the pages after it, checked as
    /// [`Stream::replay`] checks. The caller has checked the snapshot is
    /// usable ([`Registry::snapshot_usable`]); `last_recorded_at` is the
    /// snapshot's last event's, the floor for the next. The state is a
    /// fresh one (as [`Stream::replay`]'s) [restored](State::restore) from
    /// the snapshot, so what the snapshot does not hold is the factory's.
    pub fn resume(
        cover: Cover,
        snapshot: &Snapshot,
        last_recorded_at: Option<Timestamp>,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        Self::check_domain(&cover)?;
        let any = snapshot.state.clone().unwrap_or_default();
        let state = if type_name(&any.type_url) == <D::State as State>::Snapshot::full_name() {
            <D::State as State>::Snapshot::decode(&any.value[..])
                .map_err(|error| PayloadError::Decode {
                    type_url: any.type_url.clone(),
                    error,
                })
                .and_then(|wire| {
                    let mut state = registry.fresh::<D>();
                    state
                        .restore(wire)
                        .map(|()| state)
                        .map_err(|reason| PayloadError::Invalid {
                            type_url: any.type_url.clone(),
                            reason,
                        })
                })
        } else {
            Err(PayloadError::UnknownType {
                type_url: any.type_url.clone(),
            })
        }
        .map_err(|e| unreadable(&cover, snapshot.sequence, Report::of(&e)))?;
        let mut stream = Stream {
            cover,
            first: snapshot.sequence + 1,
            base: state.clone(),
            events: Vec::new(),
            state,
            last_recorded_at,
        };
        stream.fold(pages, registry)?;
        Ok(stream)
    }

    /// The stream's state as an angzarr `Snapshot`: the last sequence, the
    /// state packed as `Any` (its type URL is what a load compares), the
    /// registered rules version. `None` for an empty stream, and for a state
    /// with no snapshot form ([`State::SNAPSHOTS`] `false`).
    pub fn snapshot(&self, registry: &Registry) -> Option<Snapshot> {
        if !D::State::SNAPSHOTS {
            return None;
        }
        let sequence = self.len().checked_sub(1)?;
        Some(Snapshot {
            sequence,
            state: Some(pack(&self.state.to_snapshot())),
            rules_version: registry.rules_version(D::NAME)?,
        })
    }

    /// Hand every held event from sequence `from` on to the handlers, with
    /// the state just after it: rebuilding a projection (read models are
    /// replayed on read), or a reaction's catch-up. One pass, however many
    /// handlers. A stream resumed from a snapshot cannot hand out events
    /// before [`Stream::first`] ([`KernelError::BeforeSnapshot`]).
    pub fn dispatch(
        &self,
        from: u64,
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        if from < self.first {
            return Err(KernelError::BeforeSnapshot {
                stream: self.cover.clone(),
                first: self.first,
            });
        }
        if from >= self.len() {
            return Ok(());
        }
        Self::typed(registry)?;
        let mut state = self.base.clone();
        for envelope in &self.events {
            D::apply(&mut state, &envelope.event);
            if envelope.sequence >= from {
                let committed = Committed {
                    cover: &self.cover,
                    envelope,
                    state: &state,
                };
                for handler in handlers.iter_mut() {
                    handler.handle(&committed);
                }
            }
        }
        Ok(())
    }

    /// The held events command `command_id` produced: what deduplication
    /// finds in memory. Empty if none (never applied, or applied with no
    /// events).
    pub fn find_command(&self, command_id: &Uuid) -> &[Envelope<D::Event>] {
        &self.events[self.command_run(command_id)]
    }

    /// Where the held events of command `command_id` are: one contiguous
    /// run, or empty.
    fn command_run(&self, command_id: &Uuid) -> std::ops::Range<usize> {
        let produced = |e: &Envelope<D::Event>| e.command_id() == command_id;
        let Some(start) = self.events.iter().position(produced) else {
            return 0..0;
        };
        let run = self.events[start..]
            .iter()
            .take_while(|e| produced(e))
            .count();
        start..start + run
    }

    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    /// The events held, from [`Stream::first`].
    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    /// The sequence of the first held event: 0 unless resumed from a
    /// snapshot.
    pub fn first(&self) -> u64 {
        self.first
    }

    pub fn state(&self) -> &D::State {
        &self.state
    }

    /// The number of events, which is also the next event's sequence (the
    /// head).
    pub fn len(&self) -> u64 {
        self.first + self.events.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The last event's `recorded_at`: no later event is recorded before it.
    pub fn last_recorded_at(&self) -> Option<Timestamp> {
        self.last_recorded_at
    }

    pub fn into_parts(self) -> (Cover, Vec<Envelope<D::Event>>, D::State) {
        (self.cover, self.events, self.state)
    }
}

#[cfg(test)]
#[path = "stream.test.rs"]
mod tests;

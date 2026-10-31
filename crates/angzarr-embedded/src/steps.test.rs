//! Steps 3 to 5 driven one at a time, for tests that check each step's
//! output: a coordinator over a store, and the steps' accessors.
//! The chain ([`Kernel::handle`](crate::Kernel::handle)) runs the
//! same functions.

use angzarr_proto::v1::{ConflictResolution, EventPage, Snapshot};

use crate::check::{Identified, Sampled};
use crate::dead_letter::DeadLetterLog;
use crate::domain::Domain;
use crate::error::{Failure, KernelError};
use crate::registry::Registry;
use crate::route::Rejected;
use crate::store::{Store, StorePort};
use crate::stream::Stream;
use crate::toys::ready;

use super::{deduplicate, dispatch, load, Contextual, Deduplicated, Novel};

/// Steps 3 and 4 over a store.
pub struct Coordinator<'s, S> {
    store: &'s S,
    registry: &'s Registry,
    nonces: u64,
}

impl<'s, S: Store> Coordinator<'s, S> {
    pub fn new(store: &'s S, registry: &'s Registry) -> Coordinator<'s, S> {
        Coordinator {
            store,
            registry,
            nonces: 0,
        }
    }

    /// Step 3: load, then look the command id up.
    pub fn deduplicate(&mut self, command: Identified) -> Result<Deduplicated, Failure<Rejected>> {
        ready(deduplicate(
            &mut StorePort(self.store),
            self.registry,
            command,
        ))
    }

    /// Step 4: stamp or check the expected sequence.
    pub fn load(&mut self, command: Box<Novel>) -> Result<Contextual, KernelError> {
        load(*command, &mut self.nonces)
    }
}

impl Novel {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    /// The stream's next sequence when it was loaded.
    pub fn head(&self) -> u64 {
        self.loaded.head
    }
}

impl Contextual {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.resolution
    }

    pub fn expected_sequence(&self) -> u64 {
        self.expected
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub fn pages(&self) -> &[EventPage] {
        &self.pages
    }

    /// The events from the expected sequence to the head.
    pub fn window(&self) -> &[EventPage] {
        let start = self.pages.partition_point(|p| p.sequence < self.expected);
        &self.pages[start..]
    }

    pub fn has_nonce(&self) -> bool {
        self.nonce.is_some()
    }

    /// The stream as step 3 loaded it, typed, beside the command.
    pub fn decode<D: Domain>(self, registry: &Registry) -> Result<Prepared<D>, KernelError> {
        let stream = match &self.snapshot {
            Some(snapshot) => {
                let floor = self.last_recorded_at.filter(|_| self.pages.is_empty());
                Stream::resume(
                    self.command.cover().clone(),
                    snapshot,
                    floor,
                    &self.pages,
                    registry,
                )?
            }
            None => Stream::replay(self.command.cover().clone(), &self.pages, registry)?,
        };
        Ok(Prepared {
            contextual: self,
            stream,
        })
    }
}

/// A loaded command and its typed stream.
pub struct Prepared<D: Domain> {
    contextual: Contextual,
    stream: Stream<D>,
}

impl<D: Domain> Prepared<D> {
    pub fn command(&self) -> &Identified {
        &self.contextual.command
    }

    pub fn stream(&self) -> &Stream<D> {
        &self.stream
    }

    /// Step 5: through the router, the stamped events.
    pub fn decide(
        &self,
        sampled: &Sampled,
        registry: &Registry,
        dead_letters: &dyn DeadLetterLog,
    ) -> Result<Vec<angzarr_proto::v1::EventPage>, Failure<Rejected>> {
        crate::toys::ready(dispatch(&self.contextual, sampled, registry, dead_letters))
    }

    pub fn into_stream(self) -> Stream<D> {
        self.stream
    }
}

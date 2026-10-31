//! A store that records every write the kernel makes (the stream it
//! appended to, the positions it put, whether it landed) and fails what a
//! test tells it to: angzarr-ports' `MemoryStore` underneath. How the
//! kernel's tests see that each handling step is one write, and what is
//! left after a step fails.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_ports::memory::MemoryStore;
use angzarr_ports::Cause;

use crate::angzarr_ports::Uuid;
use crate::store::{
    EventStore, Head, PositionKey, PositionStore, Slice, SnapshotStore, StoreError, StreamRef,
    Write, Written,
};
use crate::v1::{Cover, EventPage, Snapshot};

/// One write, as the kernel made it.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct WriteRecord {
    /// The stream it appended to, if any, and how many events.
    pub wrote: Option<(Cover, usize)>,
    /// Every position it put: (handler, stream, next).
    pub positions: Vec<(String, Cover, u64)>,
    /// Whether it landed.
    pub landed: bool,
}

#[derive(Default)]
struct Log {
    writes: Vec<WriteRecord>,
    /// The writes (by order made, from 0) that fail, landing nothing, and
    /// with what.
    failing: BTreeMap<usize, StoreError>,
}

/// A `MemoryStore` whose writes are recorded. Clones share one store and
/// one log.
#[derive(Clone, Default)]
pub struct Recording {
    inner: MemoryStore,
    log: Arc<Mutex<Log>>,
}

/// The failure a test injects: the store could not say.
fn injected() -> StoreError {
    StoreError::Unavailable(Cause::new("angzarr-store-sqlite-unavailable", []))
}

impl Recording {
    pub fn new() -> Recording {
        Recording::default()
    }

    /// The store underneath, unrecorded.
    pub fn inner(&self) -> &MemoryStore {
        &self.inner
    }

    /// Every write made so far, in order.
    pub fn writes(&self) -> Vec<WriteRecord> {
        self.log().writes.clone()
    }

    /// The stream each landed write appended to, in order (`None` for a
    /// write of positions alone).
    pub fn landed(&self) -> Vec<Option<Cover>> {
        self.log()
            .writes
            .iter()
            .filter(|w| w.landed)
            .map(|w| w.wrote.as_ref().map(|(cover, _)| cover.clone()))
            .collect()
    }

    /// The `n`th write made from now on (from 0) fails, landing nothing:
    /// the store could not say.
    pub fn fail_write(&self, n: usize) {
        self.fail_write_with(n, injected());
    }

    /// The `n`th write made from now on (from 0) fails with `error`,
    /// landing nothing.
    pub fn fail_write_with(&self, n: usize, error: StoreError) {
        let mut log = self.log();
        let at = log.writes.len() + n;
        log.failing.insert(at, error);
    }

    fn log(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl EventStore for Recording {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let record = WriteRecord {
            wrote: write
                .events
                .map(|append| (append.stream.to_cover(), append.pages.len())),
            positions: write
                .positions
                .iter()
                .map(|(key, next)| (key.handler.to_string(), key.stream.to_cover(), *next))
                .collect(),
            landed: false,
        };
        let index = {
            let mut log = self.log();
            log.writes.push(record);
            log.writes.len() - 1
        };
        if let Some(error) = self.log().failing.get(&index).cloned() {
            return Err(error);
        }
        let written = self.inner.write(write).await?;
        self.log().writes[index].landed = true;
        Ok(written)
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.inner.read(stream, from, to).await
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        self.inner.head(stream).await
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.inner.find_command(stream, command_id).await
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }
}

impl SnapshotStore for Recording {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.inner.write_snapshot(stream, snapshot).await
    }
}

impl PositionStore for Recording {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        self.inner.position(key).await
    }
}

//! The storage ports are angzarr-ports': one async trait set, shared with
//! angzarr, held to one conformance suite (`angzarr_ports::contract`), with
//! an in-memory backend (`angzarr_ports::memory::MemoryStore`, feature
//! `memory`) and a SQLite one (angzarr-store-sqlite). angzarr-embedded has no store
//! trait of its own, and re-exports the shared ones here.
//!
//! **There is no unit of work.** Each handling step the kernel runs makes
//! ONE atomic [`EventStore::write`]: one stream's events at the length the
//! step loaded, with the step's positions, landing together or not at all.
//! A race is caught by that length ([`StoreError::Conflict`]); nothing is
//! held open between a step's reads and its write. A step's dedup lookup
//! comes before its write; a snapshot is its own call.
//!
//! **What the kernel needs of a store** is the base ports, [`Store`]: it
//! reads snapshots, streams and positions, looks commands up, and writes.
//! The rest (purge, read units, watch, the outbox, editions) is the
//! application's, through the store directly.
//!
//! **Opaque.** A store keeps angzarr's pages as written and never decodes a
//! payload; upcasting is the kernel's, on every read. A page the kernel
//! cannot replay is the kernel's [`KernelError::Unreadable`](super::KernelError::Unreadable);
//! a store's [`StoreError::Unreadable`] is bytes the store cannot return.
//!
//! **Absent is empty.** A stream with no events reads as an empty
//! [`Slice`] at 0: "no such stream" and "empty" are one answer, so a
//! snapshot whose stream has no events is never used.
//!
//! **Editions** are per-stream forks (14.32), the store's to enforce: a
//! command to an edition stream that was never forked is refused by the
//! store at the write ([`StoreError::EditionNotForked`]). The kernel
//! creates no edition yet.

use std::future::Future;

use angzarr_proto::v1::{Cover, EventPage, Snapshot, Uuid};

pub use angzarr_ports::{
    Append, ChangeToken, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey,
    PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice,
    SnapshotStore, Store, StoreError, StreamRef, Watch, Write, Written,
};

/// The store operations a step makes, keyed by the wire `Cover` the kernel
/// carries. A stream with no events reads as an empty slice at 0.
pub(crate) trait Port {
    /// One step's write: `pages` to `stream` at `expected`, with the step's
    /// `positions` ((handler, the stream it consumes, its next sequence)),
    /// all of it or none. Answers the stream's new length.
    fn write(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
        positions: &[(&str, &Cover, u64)],
    ) -> impl Future<Output = Result<u64, StoreError>>;

    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Slice, StoreError>>;

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>>;

    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>>;

    /// `snapshot` of `stream`, replacing the one it has.
    fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> impl Future<Output = Result<(), StoreError>>;

    /// `handler`'s next sequence on `stream`.
    fn position(
        &mut self,
        handler: &str,
        stream: &Cover,
    ) -> impl Future<Output = Result<u64, StoreError>>;

    /// A step's write of positions alone: a step that wrote no event.
    fn write_positions(
        &mut self,
        positions: &[(&str, &Cover, u64)],
    ) -> impl Future<Output = Result<(), StoreError>>;

    /// Every stream of `domain` holding an event.
    fn covers(&mut self, domain: &str) -> impl Future<Output = Result<Vec<Cover>, StoreError>>;
}

/// `positions` as the ports key them.
fn keyed<'a>(
    positions: &'a [(&'a str, &'a Cover, u64)],
) -> Result<Vec<(PositionKey<'a>, u64)>, StoreError> {
    positions
        .iter()
        .map(|(handler, stream, next)| {
            let stream = StreamRef::try_from(*stream)?;
            Ok((PositionKey { handler, stream }, *next))
        })
        .collect()
}

/// An angzarr-ports store as the kernel's [`Port`]: each `Cover` as a
/// [`StreamRef`] (a malformed one is the store's
/// [`StoreError::MalformedStream`]), a read of a stream with no events as
/// `None`, a step's write as one [`Write`].
pub(crate) struct StorePort<'s, S>(pub(crate) &'s S);

impl<S: Store> Port for StorePort<'_, S> {
    async fn write(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
        positions: &[(&str, &Cover, u64)],
    ) -> Result<u64, StoreError> {
        let positions = keyed(positions)?;
        let write = Write {
            events: Some(Append {
                stream: StreamRef::try_from(stream)?,
                expected,
                pages,
            }),
            positions: &positions,
            outbox: &[],
        };
        let written = self.0.write(write).await?;
        Ok(written.next_sequence.unwrap_or(expected))
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Slice, StoreError> {
        self.0.read(StreamRef::try_from(stream)?, from, None).await
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let command_id =
            angzarr_ports::Uuid::try_from(command_id).map_err(|_| StoreError::MalformedStream {
                stream: Box::new(stream.clone()),
            })?;
        self.0
            .find_command(StreamRef::try_from(stream)?, command_id)
            .await
    }

    async fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        self.0.read_snapshot(StreamRef::try_from(stream)?).await
    }

    async fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.0
            .write_snapshot(StreamRef::try_from(stream)?, snapshot)
            .await
    }

    async fn position(&mut self, handler: &str, stream: &Cover) -> Result<u64, StoreError> {
        let stream = StreamRef::try_from(stream)?;
        self.0.position(PositionKey { handler, stream }).await
    }

    async fn write_positions(
        &mut self,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<(), StoreError> {
        let positions = keyed(positions)?;
        self.0.write(Write::positions(&positions)).await.map(drop)
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.0.covers(domain).await
    }
}

//! One handler shape for every reader.

use crate::{Committed, Stream};

use crate::toys::{at, command, cover, registry, root, set, Ready, Shelf, ShelfEvent};

#[test]
fn handlers_receive_identity_position_metadata_and_state() {
    let registry = registry();
    let log = crate::MemoryDeadLetters::new();
    let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
    let mut seen = Vec::new();
    let mut handler = |c: &Committed<'_, Shelf>| {
        seen.push((
            c.event_ref(),
            c.envelope().recorded_at().seconds,
            c.state.items.clone(),
            matches!(c.event(), ShelfEvent::WasSet(_)),
        ))
    };
    stream
        .execute(
            command(cover("shelf", 3), set("a", 1)),
            &registry,
            &at(5),
            &log,
            &mut [&mut handler],
        )
        .ready()
        .unwrap();
    assert_eq!(seen.len(), 1);
    let (event, seconds, items, was_set) = &seen[0];
    assert_eq!(event.stream, Some(cover("shelf", 3)));
    assert_eq!(event.sequence, 0);
    assert_eq!(*seconds, 5);
    assert_eq!(items["a"], 1, "the state just after the event");
    assert!(was_set);
}

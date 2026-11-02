use super::*;

fn message() -> PeerMessage {
    PeerMessage::Piece {
        index: 5,
        begin: 7,
        block: [[4; 8].as_slice(), &[0, 3], b"untrusted payload"].concat(),
    }
}

#[test]
fn borrowed_chunk_aliases_exact_payload_and_keeps_metadata() {
    let message = message();
    let view: LiveChunkRef<'_> = LiveChunk::borrow_from_message(&message).unwrap();
    let PeerMessage::Piece { block, .. } = &message else {
        unreachable!()
    };
    assert_eq!(view.piece, 7);
    assert_eq!(view.chunk, 3);
    assert_eq!(view.piece_header, [4; 8]);
    assert_eq!(view.data, b"untrusted payload");
    assert_eq!(view.data.as_ptr(), block[10..].as_ptr());
}

#[test]
fn owned_compatibility_keeps_an_independent_payload() {
    let message = message();
    let view = LiveChunk::borrow_from_message(&message).unwrap();
    let mut owned = LiveChunk::from_message(&message).unwrap();
    assert_eq!(owned.data, view.data);
    assert_eq!(owned.piece, view.piece);
    assert_eq!(owned.chunk, view.chunk);
    assert_eq!(owned.piece_header, view.piece_header);
    assert_ne!(owned.data.as_ptr(), view.data.as_ptr());
    owned.data[0] ^= 1;
    assert_eq!(view.data, b"untrusted payload");
    assert_ne!(owned.data, view.data);
}

#[test]
fn borrowed_and_owned_parsers_preserve_short_wrong_and_empty_behavior() {
    for length in 0..10 {
        let message = PeerMessage::Piece {
            index: 5,
            begin: 7,
            block: vec![0; length],
        };
        assert!(LiveChunk::borrow_from_message(&message).is_none());
        assert!(LiveChunk::from_message(&message).is_none());
    }
    for message in [
        PeerMessage::KeepAlive,
        PeerMessage::Unknown {
            id: 7,
            payload: vec![0; 20],
        },
    ] {
        assert!(LiveChunk::borrow_from_message(&message).is_none());
        assert!(LiveChunk::from_message(&message).is_none());
    }
    let message = PeerMessage::Piece {
        index: 5,
        begin: 7,
        block: vec![0; 10],
    };
    let view = LiveChunk::borrow_from_message(&message).unwrap();
    assert!(view.data.is_empty());
    assert!(LiveChunk::from_message(&message).unwrap().data.is_empty());
}

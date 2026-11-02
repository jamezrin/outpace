//! Counts only this thread's synchronous decode allocation requests.
use ace_wire::message::{PeerMessage, MAX_FRAME_LEN};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingSystem;

fn record() {
    let _ = COUNT.try_with(|count| {
        if let Some(current) = count.get() {
            count.set(Some(current + 1));
        }
    });
}

// SAFETY: Every operation delegates the original arguments to System without
// changing allocation/deallocation behavior. The thread-local counter allocates
// no memory and is ignored after TLS destruction.
unsafe impl GlobalAlloc for CountingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingSystem = CountingSystem;

struct Scope;
impl Drop for Scope {
    fn drop(&mut self) {
        COUNT.with(|count| count.set(None));
    }
}

fn measured<T>(run: impl FnOnce() -> T) -> (T, usize) {
    COUNT.with(|count| assert!(count.replace(Some(0)).is_none()));
    let scope = Scope;
    let result = run();
    let count = COUNT.with(|count| count.get().unwrap());
    drop(scope);
    (result, count)
}

fn decode_case(message: PeerMessage, allocations: usize) {
    let frame = message.encode();
    let (decoded, count) = measured(|| PeerMessage::decode(std::hint::black_box(&frame)));
    assert_eq!(decoded.unwrap(), Some((message, frame.len())));
    assert_eq!(count, allocations, "unexpected allocation requests");
}

#[test]
fn fixed_size_typed_frames_need_no_payload_allocation() {
    for message in [
        PeerMessage::KeepAlive,
        PeerMessage::Choke,
        PeerMessage::Unchoke,
        PeerMessage::Interested,
        PeerMessage::NotInterested,
        PeerMessage::Have(42),
        PeerMessage::Request {
            index: 3,
            begin: 8,
            length: 16,
        },
        PeerMessage::Cancel {
            index: 3,
            begin: 8,
            length: 16,
        },
    ] {
        decode_case(message, 0);
    }
}

#[test]
fn typed_payloads_allocate_only_the_owned_result() {
    for size in [64, 16 * 1024] {
        for message in [
            PeerMessage::Piece {
                index: 3,
                begin: 8,
                block: vec![17; size],
            },
            PeerMessage::Bitfield(vec![17; size]),
            PeerMessage::Extended {
                ext_id: 2,
                payload: vec![17; size],
            },
        ] {
            decode_case(message, 1);
        }
    }
}

#[test]
fn unknown_and_wrong_length_frames_keep_exact_owned_payloads() {
    for id in [0, 1, 2, 3, 4, 6, 7, 8, 127] {
        // Three bytes cannot be a valid payload for these modeled IDs.
        decode_case(
            PeerMessage::Unknown {
                id,
                payload: vec![11, 22, 33],
            },
            1,
        );
    }
    for message in [
        PeerMessage::Bitfield(Vec::new()),
        PeerMessage::Piece {
            index: 3,
            begin: 8,
            block: Vec::new(),
        },
        PeerMessage::Extended {
            ext_id: 2,
            payload: Vec::new(),
        },
        PeerMessage::Unknown {
            id: 127,
            payload: Vec::new(),
        },
        PeerMessage::Unknown {
            id: 20,
            payload: Vec::new(),
        },
    ] {
        decode_case(message, 0);
    }
}

#[test]
fn incomplete_oversized_and_coalesced_frames_preserve_boundaries() {
    let message = PeerMessage::Piece {
        index: 3,
        begin: 8,
        block: vec![17; 64],
    };
    let frame = message.encode();
    for length in 0..frame.len() {
        let (decoded, count) = measured(|| PeerMessage::decode(&frame[..length]));
        assert!(decoded.unwrap().is_none());
        assert_eq!(count, 0);
    }
    let oversized = ((MAX_FRAME_LEN + 1) as u32).to_be_bytes();
    let (decoded, count) = measured(|| PeerMessage::decode(&oversized));
    assert!(decoded.is_err());
    assert_eq!(count, 0);
    let coalesced = [frame.as_slice(), &PeerMessage::Have(99).encode()].concat();
    let (decoded, count) = measured(|| PeerMessage::decode(&coalesced));
    assert_eq!(decoded.unwrap(), Some((message, frame.len())));
    assert_eq!(count, 1);
    assert_eq!(
        PeerMessage::decode(&coalesced[frame.len()..]).unwrap(),
        Some((PeerMessage::Have(99), 9))
    );
}

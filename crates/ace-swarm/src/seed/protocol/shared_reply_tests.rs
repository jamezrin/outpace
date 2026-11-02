use super::*;
use crate::store::BackendKind;

#[tokio::test]
async fn shared_preparation_preserves_exact_wire_profiles_and_block_replies() {
    for disk in [false, true] {
        for stored in [[0; 8], [3; 8]] {
            let directory = tempfile::tempdir().unwrap();
            let backend = if disk {
                BackendKind::Disk {
                    dir: directory.path().join("cache"),
                }
            } else {
                BackendKind::Memory
            };
            let store = Arc::new(Mutex::new(
                PieceStore::with_backend(8, 4, 8, backend).unwrap(),
            ));
            PieceStore::shared_put_complete_piece(&store, 7, stored, b"abcdefgh")
                .await
                .unwrap();
            let request = PeerMessage::Unknown {
                id: 6,
                payload: [
                    5_u32.to_be_bytes().as_slice(),
                    7_u32.to_be_bytes().as_slice(),
                    &[0, 1],
                ]
                .concat(),
            };
            for (profile, stream, header) in [
                (
                    ServeProfile::Inbound {
                        fallback_header: [9; 8],
                    },
                    5_u32,
                    if stored == [0; 8] { [9; 8] } else { stored },
                ),
                (ServeProfile::Reciprocal, 0, stored),
            ] {
                let reply = prepare_reply(&request, Some(&store), profile)
                    .await
                    .unwrap();
                let mut expected = vec![0, 0, 0, 23, 7];
                expected.extend(stream.to_be_bytes());
                expected.extend(7_u32.to_be_bytes());
                expected.extend(header);
                expected.extend([0, 1]);
                expected.extend(b"efgh");
                assert_eq!(reply.message.encode(), expected);
                assert_eq!(reply.media_bytes, 4);
            }
            let block = PeerMessage::Request {
                index: 7,
                begin: 2,
                length: 4,
            };
            let reply = prepare_reply(
                &block,
                Some(&store),
                ServeProfile::Inbound {
                    fallback_header: [9; 8],
                },
            )
            .await
            .unwrap();
            assert_eq!(
                reply.message,
                PeerMessage::Piece {
                    index: 7,
                    begin: 2,
                    block: b"cdef".to_vec()
                }
            );
            assert_eq!(reply.media_bytes, 4);
            assert!(
                prepare_reply(&block, Some(&store), ServeProfile::Reciprocal)
                    .await
                    .is_none()
            );
        }
    }
}

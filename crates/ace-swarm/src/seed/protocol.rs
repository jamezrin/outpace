//! Request/reply mechanics shared by inbound and reciprocal serving.
//!
//! Callers retain connection eligibility, transport writes and accounting. In particular,
//! reciprocal serving is outside the inbound coordinator budget and can serve before interest.
use crate::store::PieceStore;
use ace_wire::live_codec::build_piece;
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Copy)]
pub(crate) enum ServeProfile {
    /// Echo the request stream, replace zero stored headers, and allow BT block requests.
    Inbound { fallback_header: [u8; 8] },
    /// Live requests only, stream zero, and the stored header exactly (including zero).
    Reciprocal,
}

pub(crate) struct PreparedReply {
    pub(crate) message: PeerMessage,
    /// Stored payload bytes, excluding the live wire header; used at caller success boundaries.
    pub(crate) media_bytes: usize,
}

pub(crate) async fn prepare_reply(
    request: &PeerMessage,
    store: Option<&Arc<Mutex<PieceStore>>>,
    profile: ServeProfile,
) -> Option<PreparedReply> {
    let store = store?;
    match request {
        PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 => {
            let requested_stream = u32::from_be_bytes(payload[0..4].try_into().ok()?);
            let piece = u32::from_be_bytes(payload[4..8].try_into().ok()?);
            let chunk = u16::from_be_bytes(payload[8..10].try_into().ok()?);
            let reciprocal;
            let ordinary;
            let (data, stored_header) = match profile {
                ServeProfile::Reciprocal => {
                    reciprocal =
                        PieceStore::shared_reciprocal_chunk(store, piece as u64, chunk).await?;
                    (&reciprocal.data, reciprocal.header)
                }
                ServeProfile::Inbound { .. } => {
                    ordinary = PieceStore::shared_chunk(store, piece as u64, chunk).await?;
                    (&ordinary.0, ordinary.1)
                }
            };
            let (stream, header) = match profile {
                ServeProfile::Inbound { fallback_header } => (
                    requested_stream,
                    if stored_header == [0; 8] {
                        fallback_header
                    } else {
                        stored_header
                    },
                ),
                ServeProfile::Reciprocal => (0, stored_header),
            };
            Some(PreparedReply {
                message: build_piece(stream, piece, chunk, header, data),
                media_bytes: data.len(),
            })
        }
        PeerMessage::Request {
            index,
            begin,
            length,
        } if matches!(profile, ServeProfile::Inbound { .. }) => {
            let block = PieceStore::shared_block(store, *index as u64, *begin, *length).await?;
            Some(PreparedReply {
                media_bytes: block.len(),
                message: PeerMessage::Piece {
                    index: *index,
                    begin: *begin,
                    block,
                },
            })
        }
        _ => None,
    }
}

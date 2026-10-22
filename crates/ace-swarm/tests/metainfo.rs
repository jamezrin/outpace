use ace_swarm::resolve::{stream_info_from_transport, vod_info_from_transport};
use ace_wire::message::PeerMessage;
use bytes::Bytes;
use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
use sha1::{Digest, Sha1};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const RAW: &[u8] = include_bytes!("../../../tests/vectors/metainfo/single-file-unsorted.torrent");

fn bstr(bytes: &[u8]) -> Vec<u8> {
    let mut out = format!("{}:", bytes.len()).into_bytes();
    out.extend_from_slice(bytes);
    out
}

fn dict(entries: &[(&[u8], Vec<u8>)]) -> Vec<u8> {
    let mut out = vec![b'd'];
    for (key, value) in entries {
        out.extend(bstr(key));
        out.extend(value);
    }
    out.push(b'e');
    out
}

fn int(value: u64) -> Vec<u8> {
    format!("i{value}e").into_bytes()
}

fn tracker_list(url: &[u8]) -> Vec<u8> {
    let mut out = vec![b'l'];
    out.extend(bstr(url));
    out.push(b'e');
    out
}

fn encrypted_transport(descriptor: &[u8]) -> Vec<u8> {
    type Enc = cbc::Encryptor<aes::Aes128>;
    let body = Enc::new_from_slices(
        &ace_wire::transport::TRANSPORT_KEY,
        &ace_wire::transport::TRANSPORT_IV,
    )
    .unwrap()
    .encrypt_padded_vec::<Pkcs7>(descriptor);
    let mut out = b"AceStreamTransport\x00\x02".to_vec();
    out.extend(body);
    out
}

#[test]
fn raw_metainfo_is_vod_input_and_not_live_input() {
    let vod = vod_info_from_transport(RAW).expect("raw torrent should resolve as VOD");
    assert_eq!(vod.piece_length, 16_384);
    assert_eq!(vod.total_length, 16_385);
    assert_eq!(vod.piece_hashes, vec![[0xA5; 20], [0x5A; 20]]);
    assert_eq!(
        vod.peer_protocol,
        ace_wire::handshake::HandshakeProtocol::BitTorrent
    );
    assert!(stream_info_from_transport(RAW).is_err());
}

#[test]
fn legacy_encrypted_live_and_vod_transports_keep_their_classification() {
    let live = dict(&[
        (b"piece_length", int(16_384)),
        (b"chunk_length", int(16_384)),
        (b"name", bstr(b"synthetic live")),
        (b"authmethod", bstr(b"None")),
        (b"pubkey", bstr(b"")),
        (b"bitrate", int(100_000)),
        (b"trackers", tracker_list(b"udp://tracker.invalid:80")),
    ]);
    let live = encrypted_transport(&live);
    assert_eq!(
        stream_info_from_transport(&live).unwrap().piece_length,
        16_384
    );
    assert!(vod_info_from_transport(&live).is_err());

    let vod = dict(&[
        (b"piece_length", int(16_384)),
        (b"chunk_length", int(16_384)),
        (b"length", int(16_385)),
        (b"pieces", bstr(&[0x31; 40])),
        (b"name", bstr(b"synthetic vod")),
        (b"authmethod", bstr(b"None")),
        (b"pubkey", bstr(b"")),
        (b"bitrate", int(100_000)),
        (b"trackers", tracker_list(b"udp://tracker.invalid:80")),
    ]);
    let wrapped_vod = encrypted_transport(&vod);
    let vod_info = vod_info_from_transport(&wrapped_vod).unwrap();
    assert_eq!(vod_info.total_length, 16_385);
    assert_eq!(
        vod_info.peer_protocol,
        ace_wire::handshake::HandshakeProtocol::AceStream
    );
    assert!(stream_info_from_transport(&wrapped_vod).is_ok());
}

#[tokio::test]
async fn vod_download_uses_standard_bittorrent_handshake_and_reads_final_short_piece() {
    let content = vec![0x37; 16_385];
    let mut hashes = Vec::new();
    for piece in content.chunks(16_384) {
        hashes.extend_from_slice(&Sha1::digest(piece));
    }
    let info_bytes = dict(&[
        (b"length", int(content.len() as u64)),
        (b"name", bstr(b"synthetic payload.bin")),
        (b"piece length", int(16_384)),
        (b"pieces", bstr(&hashes)),
    ]);
    let raw_metainfo = dict(&[(b"info", info_bytes)]);
    let info = vod_info_from_transport(&raw_metainfo).expect("synthetic raw metainfo resolves");
    assert_eq!(
        info.peer_protocol,
        ace_wire::handshake::HandshakeProtocol::BitTorrent
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let expected_content = content.clone();
    let expected_infohash = info.infohash;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut client_handshake = [0u8; 68];
        socket.read_exact(&mut client_handshake).await.unwrap();
        assert_eq!(&client_handshake[..20], b"\x13BitTorrent protocol");
        assert_eq!(&client_handshake[28..48], expected_infohash.as_slice());

        // Literal standard BitTorrent handshake: pstrlen, pstr, reserved, infohash, peer id.
        let mut reply = b"\x13BitTorrent protocol".to_vec();
        reply.extend([0u8; 8]);
        reply.extend(expected_infohash);
        reply.extend(*b"-UT0001-123456789012");
        socket.write_all(&reply).await.unwrap();
        socket
            .write_all(&PeerMessage::Bitfield(vec![0xC0]).encode())
            .await
            .unwrap();
        socket
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .unwrap();

        let mut buffered = Vec::new();
        let mut scratch = [0u8; 4096];
        loop {
            let count = socket.read(&mut scratch).await.unwrap();
            if count == 0 {
                return;
            }
            buffered.extend_from_slice(&scratch[..count]);
            while let Some((message, used)) = PeerMessage::decode(&buffered).unwrap() {
                buffered.drain(..used);
                if let PeerMessage::Request {
                    index,
                    begin,
                    length,
                } = message
                {
                    let start = index as usize * 16_384 + begin as usize;
                    let end = start + length as usize;
                    socket
                        .write_all(
                            &PeerMessage::Piece {
                                index,
                                begin,
                                block: expected_content[start..end].to_vec(),
                            }
                            .encode(),
                        )
                        .await
                        .unwrap();
                }
            }
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(4);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ace_swarm::vod::download_vod(info, vec![addr], tx),
    )
    .await;
    let mut downloaded = Vec::new();
    while let Some(chunk) = rx.recv().await {
        downloaded.extend_from_slice(&chunk);
    }
    peer.abort();

    assert!(
        result.is_ok(),
        "VOD download should finish within the local peer timeout"
    );
    assert!(
        result.unwrap().is_ok(),
        "standard BitTorrent peer should be accepted"
    );
    assert_eq!(downloaded, content);
}

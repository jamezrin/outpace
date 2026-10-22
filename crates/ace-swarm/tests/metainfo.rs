use ace_swarm::resolve::{stream_info_from_transport, vod_info_from_transport};
use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};

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
    let vod = encrypted_transport(&vod);
    assert_eq!(vod_info_from_transport(&vod).unwrap().total_length, 16_385);
    assert!(stream_info_from_transport(&vod).is_ok());
}

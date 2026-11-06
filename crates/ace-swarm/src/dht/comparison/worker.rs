//! Fixed offline custom-walker entry, compiled only in libtest on Linux.
use std::{collections::HashMap, ffi::OsString, net::SocketAddrV4};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerError {
    Namespace,
}
#[path = "worker_isolation.rs"]
mod isolation;
struct Request {
    hash: [u8; 20],
    seeds: Vec<SocketAddrV4>,
}
fn parse_environment(
    entries: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Request, ()> {
    let mut values = HashMap::new();
    for (key, value) in entries {
        if !key.as_encoded_bytes().starts_with(b"OUTPACE_DHT_COMPARE_") {
            continue;
        }
        let key = key
            .to_str()
            .ok_or(())?
            .strip_prefix("OUTPACE_DHT_COMPARE_")
            .ok_or(())?;
        if !matches!(key, "MODE" | "ROLE" | "TARGET" | "SEEDS") {
            return Err(());
        }
        let value = value.to_str().ok_or(())?;
        let limit = match key {
            "TARGET" => 40,
            "SEEDS" => 256,
            _ => 16,
        };
        if value.len() > limit || values.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(());
        }
    }
    if values.len() != 4
        || values["MODE"] != "offline-v1"
        || values["ROLE"] != "custom"
        || values["TARGET"].len() != 40
        || values["SEEDS"].is_empty()
    {
        return Err(());
    }
    let mut hash = [0; 20];
    for (out, pair) in hash
        .iter_mut()
        .zip(values["TARGET"].as_bytes().as_chunks::<2>().0)
    {
        let digit = |b: u8| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err(()),
        };
        *out = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    let mut seeds = Vec::with_capacity(4);
    for text in values["SEEDS"].split(',') {
        let seed: SocketAddrV4 = text.parse().map_err(|_| ())?;
        if seeds.len() == 4 || !seed.ip().is_loopback() || seed.port() == 0 || seeds.contains(&seed)
        {
            return Err(());
        }
        seeds.push(seed);
    }
    Ok(Request { hash, seeds })
}
fn entry<E, V>(environment: E, output: &mut impl std::io::Write) -> i32
where
    E: FnOnce() -> V,
    V: IntoIterator<Item = (OsString, OsString)>,
{
    if isolation::attest_offline().is_err() {
        return 65;
    }
    let Ok(request) = parse_environment(environment()) else {
        return 64;
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return 70;
    };
    let mut value = runtime.block_on(super::run_comparison(&request.hash, request.seeds));
    value["schema_version"] = 1.into();
    value["candidate"] = "custom".into();
    value["diagnostics_unavailable_reason"] =
        if value["nodes_queried"].is_u64() && value["valid_responses"].is_u64() {
            serde_json::Value::Null
        } else {
            "not_collected".into()
        };
    if !value["nodes_queried"].is_u64() || !value["valid_responses"].is_u64() {
        value["nodes_queried"] = serde_json::Value::Null;
        value["valid_responses"] = serde_json::Value::Null;
    }
    let Ok(mut bytes) = serde_json::to_vec(&value) else {
        return 74;
    };
    if bytes.len() > 8192 {
        return 74;
    }
    bytes.push(b'\n');
    if output
        .write_all(b"\nDHT_COMPARE_RECORD ")
        .and_then(|()| output.write_all(&bytes))
        .and_then(|()| output.flush())
        .is_err()
    {
        return 74;
    }
    0
}
#[test]
#[ignore = "fixed worker: exact compiled case only through owned offline namespace"]
fn custom_offline_worker() {
    let status = entry(std::env::vars_os, &mut std::io::stdout().lock());
    assert_eq!(status, 0, "custom worker denied or failed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    fn env() -> Vec<(OsString, OsString)> {
        [
            ("MODE", "offline-v1"),
            ("ROLE", "custom"),
            ("TARGET", "0123456789abcdef0123456789abcdef01234567"),
            ("SEEDS", "127.0.0.1:9001"),
        ]
        .map(|(k, v)| {
            (
                OsString::from(format!("OUTPACE_DHT_COMPARE_{k}")),
                OsString::from(v),
            )
        })
        .to_vec()
    }
    #[test]
    fn custom_boundary_denies_bad_environment() {
        assert!(parse_environment(env()).is_ok());
        for (key, value) in [
            ("MODE", "live-v1"),
            ("ROLE", "mainline"),
            ("TARGET", "sensitive-invalid"),
            ("SEEDS", "192.0.2.1:5"),
            ("SEEDS", "127.0.0.1:0"),
            ("SEEDS", "127.0.0.1:5,127.0.0.1:5"),
        ] {
            let mut entries = env();
            entries
                .iter_mut()
                .find(|(k, _)| k == &OsString::from(format!("OUTPACE_DHT_COMPARE_{key}")))
                .unwrap()
                .1 = value.into();
            assert!(parse_environment(entries).is_err());
        }
        let mut duplicate = env();
        duplicate.push(duplicate[0].clone());
        assert!(parse_environment(duplicate).is_err());
        let mut unknown = env();
        unknown.push(("OUTPACE_DHT_COMPARE_EXTRA".into(), "secret".into()));
        assert!(parse_environment(unknown).is_err());
        assert!(parse_environment(env().into_iter().take(3)).is_err());
    }
    #[test]
    fn unconfined_entry_never_reads_environment() {
        assert_ne!(std::process::id(), 1);
        let mut output = Vec::new();
        assert_eq!(
            entry(
                || -> Vec<(OsString, OsString)> { panic!("read before gate") },
                &mut output
            ),
            65
        );
        assert!(output.is_empty());
    }
}

#[test]
#[ignore = "same entry target witness only through owned offline namespace"]
fn custom_confined_entry_target() {
    use super::Bencode;
    use std::{collections::BTreeMap, net::UdpSocket, time::Duration};
    isolation::attest_offline().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let seed = socket.local_addr().unwrap().to_string();
    let server = std::thread::spawn(move || {
        let mut bytes = [0; 65536];
        let (n, from) = socket.recv_from(&mut bytes).unwrap();
        let query = Bencode::parse(&bytes[..n]).unwrap();
        assert_eq!(
            query.get(b"q").and_then(Bencode::as_bytes),
            Some(b"get_peers".as_slice())
        );
        let expected = [
            1, 35, 69, 103, 137, 171, 205, 239, 1, 35, 69, 103, 137, 171, 205, 239, 1, 35, 69, 103,
        ];
        assert_eq!(
            query
                .get(b"a")
                .unwrap()
                .get(b"info_hash")
                .and_then(Bencode::as_bytes),
            Some(expected.as_slice())
        );
        let peers = (10001u16..=10010)
            .map(|port| {
                let mut p = vec![127, 0, 0, 1];
                p.extend(port.to_be_bytes());
                Bencode::Bytes(p)
            })
            .collect();
        let reply = Bencode::Dict(BTreeMap::from([
            (b"t".to_vec(), query.get(b"t").unwrap().clone()),
            (b"y".to_vec(), Bencode::Bytes(b"r".to_vec())),
            (
                b"r".to_vec(),
                Bencode::Dict(BTreeMap::from([
                    (b"id".to_vec(), Bencode::Bytes(vec![42; 20])),
                    (b"values".to_vec(), Bencode::List(peers)),
                    (b"token".to_vec(), Bencode::Bytes(b"synthetic".to_vec())),
                ])),
            ),
        ]));
        socket.send_to(&reply.encode(), from).unwrap();
    });
    let mut output = Vec::new();
    let status = entry(
        || {
            [
                ("MODE", "offline-v1".to_owned()),
                ("ROLE", "custom".to_owned()),
                (
                    "TARGET",
                    "0123456789abcdef0123456789abcdef01234567".to_owned(),
                ),
                ("SEEDS", seed),
            ]
            .map(|(k, v)| {
                (
                    OsString::from(format!("OUTPACE_DHT_COMPARE_{k}")),
                    OsString::from(v),
                )
            })
        },
        &mut output,
    );
    let joined = server.join();
    assert!(joined.is_ok(), "owned seed failed");
    assert_eq!(status, 0);
    let bytes = output.strip_prefix(b"\nDHT_COMPARE_RECORD ").unwrap();
    let record: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(record["terminal"], "target");
    assert_eq!(record["peer_count"], 10);
    assert_eq!(record["batch_sizes"], serde_json::json!([10]));
    assert_eq!(record["target_overshoot"], 2);
    print!("{}", String::from_utf8(output).unwrap());
}

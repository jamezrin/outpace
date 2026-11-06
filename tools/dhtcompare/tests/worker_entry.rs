#![cfg(feature = "workers")]
use outpace_dht_compare::worker::entry;
use std::ffi::OsString;
#[test]
fn arguments_deny_before_environment_or_output() {
    for args in [
        vec![],
        vec!["--offline-worker", "other"],
        vec!["--offline-worker", "n0", "extra"],
    ] {
        let mut output = Vec::new();
        let status = entry(
            args.into_iter().map(OsString::from),
            || -> Vec<(OsString, OsString)> {
                panic!("environment read before argument admission")
            },
            &mut output,
        );
        assert_eq!(status, 64);
        assert!(output.is_empty());
    }
}
#[test]
fn unconfined_valid_mode_denies_before_environment() {
    assert_ne!(std::process::id(), 1);
    for role in ["mainline", "n0"] {
        let mut output = Vec::new();
        let status = entry(
            ["--offline-worker", role].map(OsString::from),
            || -> Vec<(OsString, OsString)> {
                panic!("environment read before namespace admission")
            },
            &mut output,
        );
        assert_eq!(status, 65);
        assert!(output.is_empty());
    }
}

// Same production entry, gate, adapter and encoder; deliberately not a claim
// about positive OS-environment/stdout integration through binary main().
fn confined_entry(role: &str) {
    use serde_bencode::value::Value;
    use std::{
        collections::HashMap,
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };
    outpace_dht_compare::worker::attest_offline().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let seed = socket.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let done = stop.clone();
    let server = std::thread::spawn(move || {
        let mut packet = [0; 65536];
        let mut witnessed = false;
        while !done.load(Ordering::Acquire) {
            let (n, from) = match socket.recv_from(&mut packet) {
                Ok(message) => message,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue
                }
                Err(error) => panic!("fixture receive: {error}"),
            };
            let Value::Dict(query) = serde_bencode::from_bytes::<Value>(&packet[..n]).unwrap()
            else {
                panic!("fixture query not dictionary")
            };
            let method = match query.get(b"q".as_slice()) {
                Some(Value::Bytes(m)) => m.as_slice(),
                _ => panic!("missing method"),
            };
            assert!(matches!(method, b"ping" | b"find_node" | b"get_peers"));
            let mut response = HashMap::from([
                (b"id".to_vec(), Value::Bytes(vec![42; 20])),
                (b"nodes".to_vec(), Value::Bytes(Vec::new())),
            ]);
            if method == b"get_peers" {
                let Some(Value::Dict(arguments)) = query.get(b"a".as_slice()) else {
                    panic!("missing arguments")
                };
                let expected = outpace_dht_compare::WorkerInput::parse(
                    "0123456789abcdef0123456789abcdef01234567",
                    "127.0.0.1:1",
                )
                .unwrap();
                assert_eq!(
                    arguments.get(b"info_hash".as_slice()),
                    Some(&Value::Bytes(expected.infohash().to_vec()))
                );
                witnessed = true;
                let peers = (10001u16..=10010)
                    .map(|port| {
                        let mut address = vec![127, 0, 0, 1];
                        address.extend(port.to_be_bytes());
                        Value::Bytes(address)
                    })
                    .collect();
                response.insert(b"values".to_vec(), Value::List(peers));
                response.insert(b"token".to_vec(), Value::Bytes(b"synthetic".to_vec()));
            }
            let reply = Value::Dict(HashMap::from([
                (b"t".to_vec(), query.get(b"t".as_slice()).unwrap().clone()),
                (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
                (b"r".to_vec(), Value::Dict(response)),
            ]));
            socket
                .send_to(&serde_bencode::to_bytes(&reply).unwrap(), from)
                .unwrap();
        }
        witnessed
    });
    let mut output = Vec::new();
    let status = entry(
        ["--offline-worker", role].map(OsString::from),
        || {
            [
                ("MODE", "offline-v1".to_owned()),
                ("ROLE", role.to_owned()),
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
    stop.store(true, Ordering::Release);
    let witnessed = server.join().expect("owned seed failed");
    assert_eq!(status, 0);
    assert!(witnessed, "no exact target query");
    let record: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(record["candidate"], role);
    assert_eq!(record["terminal"], "target");
    assert_eq!(record["peer_count"], 10);
    assert_eq!(record["batch_sizes"], serde_json::json!([10]));
    assert_eq!(record["target_overshoot"], 2);
    println!(
        "WORKER_FIXTURE_RECORD {}",
        String::from_utf8(output).unwrap().trim_end()
    );
}
#[test]
#[ignore = "exact compiled case only through owned offline namespace helper"]
fn mainline_confined_entry_target() {
    confined_entry("mainline");
}
#[test]
#[ignore = "exact compiled case only through owned offline namespace helper"]
fn n0_confined_entry_target() {
    confined_entry("n0");
}

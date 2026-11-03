#![cfg(feature = "candidates")]
use outpace_dht_compare::{
    candidates::{run, Candidate, Terminal},
    WorkerInput,
};
use serde_bencode::value::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::UdpSocket;

fn require_isolation() {
    assert_eq!(std::process::id(), 1, "requires owned PID namespace");
    let links = std::process::Command::new("/usr/bin/ip")
        .args(["-o", "link", "show"])
        .output()
        .unwrap();
    assert!(links.status.success());
    let links = String::from_utf8(links.stdout).unwrap();
    assert_eq!(links.lines().count(), 1, "external interface present");
    assert!(links.contains("lo:"));
    let routes = std::process::Command::new("/usr/bin/ip")
        .args(["route", "show"])
        .output()
        .unwrap();
    assert!(
        routes.status.success() && routes.stdout.is_empty(),
        "external routes present"
    );
}

async fn fixture(candidate: Candidate) {
    require_isolation();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let seed = socket.local_addr().unwrap().to_string();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let seen = queries.clone();
    let server = tokio::spawn(async move {
        let mut packet = [0; 65536];
        loop {
            let (n, from) = socket.recv_from(&mut packet).await.unwrap();
            let Value::Dict(query) = serde_bencode::from_bytes::<Value>(&packet[..n]).unwrap()
            else {
                panic!("not dictionary")
            };
            let Some(Value::Bytes(q)) = query.get(b"q".as_slice()) else {
                continue;
            };
            seen.lock().unwrap().push(q.clone());
            assert!(
                matches!(q.as_slice(), b"find_node" | b"get_peers" | b"ping"),
                "unexpected query"
            );
            let mut response = HashMap::from([
                (b"id".to_vec(), Value::Bytes(vec![42; 20])),
                (b"nodes".to_vec(), Value::Bytes(Vec::new())),
            ]);
            if q == b"get_peers" {
                let peers = (10001u16..=10010)
                    .map(|port| {
                        let mut p = vec![127, 0, 0, 1];
                        p.extend(port.to_be_bytes());
                        Value::Bytes(p)
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
                .await
                .unwrap();
        }
    });
    let input = WorkerInput::parse("0123456789abcdef0123456789abcdef01234567", &seed).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(4), run(candidate, &input)).await;
    server.abort();
    let joined = server.await;
    assert!(
        joined.is_err_and(|e| e.is_cancelled()),
        "fixture task failed before cleanup"
    );
    let record = result.expect("candidate did not reach target within fixture bound");
    assert_eq!(record.terminal, Terminal::Target);
    assert_eq!(
        record.peer_count, 10,
        "crossing batch must be retained atomically"
    );
    assert!(record.first_peer.is_some());
    assert!(record.elapsed < Duration::from_secs(4));
    assert!(queries.lock().unwrap().iter().any(|q| q == b"get_peers"));
}

#[tokio::test]
#[ignore = "run compiled test only via offline namespace helper"]
async fn mainline_real_udp_target() {
    fixture(Candidate::Mainline).await
}
#[tokio::test]
#[ignore = "run compiled test only via offline namespace helper"]
async fn n0_real_udp_target() {
    fixture(Candidate::N0).await
}

fn uncorrelated_txid(original: &[u8]) -> Vec<u8> {
    let mut changed = original.to_vec();
    assert!(!changed.is_empty());
    changed[0] ^= 128;
    changed
}

#[test]
fn uncorrelated_fixture_preserves_transaction_schema() {
    for original in [&[0u8, 1][..], &[0u8, 0, 0, 1][..]] {
        let changed = uncorrelated_txid(original);
        assert_eq!(changed.len(), original.len());
        assert_ne!(changed, original);
    }
}

// These are qualification contracts for the real libraries, not mock adapters.
// Removing transaction correlation or peer deduplication must fail the sparse case.
async fn fault_fixture(candidate: Candidate, silent: bool) {
    require_isolation();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let seed = socket.local_addr().unwrap().to_string();
    let queries = Arc::new(Mutex::new(0usize));
    let seen = queries.clone();
    let server = tokio::spawn(async move {
        let mut packet = [0; 65536];
        loop {
            let (n, from) = socket.recv_from(&mut packet).await.unwrap();
            let Value::Dict(query) = serde_bencode::from_bytes::<Value>(&packet[..n]).unwrap()
            else {
                panic!("not dictionary")
            };
            let Some(Value::Bytes(q)) = query.get(b"q".as_slice()) else {
                panic!("missing method")
            };
            assert!(matches!(
                q.as_slice(),
                b"find_node" | b"get_peers" | b"ping"
            ));
            *seen.lock().unwrap() += 1;
            if silent {
                continue;
            }
            let txid = query.get(b"t".as_slice()).unwrap().clone();
            let reply = |txid, ports: Vec<u16>| {
                let peers = ports
                    .into_iter()
                    .map(|port| {
                        let mut p = vec![127, 0, 0, 1];
                        p.extend(port.to_be_bytes());
                        Value::Bytes(p)
                    })
                    .collect();
                let mut response = HashMap::from([
                    (b"id".to_vec(), Value::Bytes(vec![42; 20])),
                    (b"nodes".to_vec(), Value::Bytes(Vec::new())),
                ]);
                if q == b"get_peers" {
                    response.insert(b"values".to_vec(), Value::List(peers));
                    response.insert(b"token".to_vec(), Value::Bytes(b"synthetic".to_vec()));
                }
                serde_bencode::to_bytes(&Value::Dict(HashMap::from([
                    (b"t".to_vec(), txid),
                    (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
                    (b"r".to_vec(), Value::Dict(response)),
                ])))
                .unwrap()
            };
            socket.send_to(b"not-bencode", from).await.unwrap();
            if q == b"get_peers" {
                // Deliberately uncorrelated response advertises enough unique peers
                // to falsely satisfy the target if the library accepts it.
                socket
                    .send_to(
                        &reply(
                            Value::Bytes(uncorrelated_txid(match &txid {
                                Value::Bytes(value) => value,
                                _ => panic!("invalid query transaction"),
                            })),
                            (10001..=10010).collect(),
                        ),
                        from,
                    )
                    .await
                    .unwrap();
            }
            socket
                .send_to(&reply(txid, vec![10001; 10]), from)
                .await
                .unwrap();
        }
    });
    let input = WorkerInput::parse("0123456789abcdef0123456789abcdef01234567", &seed).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(16), run(candidate, &input)).await;
    server.abort();
    assert!(
        server.await.is_err_and(|e| e.is_cancelled()),
        "fixture task failed before cleanup"
    );
    let record = result.expect("absolute lookup deadline not honored");
    assert_ne!(record.terminal, Terminal::Target);
    assert!(matches!(
        record.terminal,
        Terminal::Deadline | Terminal::Exhausted
    ));
    assert_eq!(record.peer_count, usize::from(!silent));
    assert_eq!(record.first_peer.is_none(), silent);
    assert!(record.elapsed < Duration::from_secs(16));
    assert!(*queries.lock().unwrap() > 0, "no actual UDP query observed");
}

#[tokio::test]
#[ignore = "run compiled test only via offline namespace helper"]
async fn mainline_real_udp_silent() {
    fault_fixture(Candidate::Mainline, true).await
}
#[tokio::test]
#[ignore = "run compiled test only via offline namespace helper"]
async fn n0_real_udp_silent() {
    fault_fixture(Candidate::N0, true).await
}
#[tokio::test]
#[ignore = "run compiled test only via offline namespace helper"]
async fn mainline_real_udp_malformed_uncorrelated_duplicates() {
    fault_fixture(Candidate::Mainline, false).await
}
#[tokio::test]
#[ignore = "run compiled test only via offline namespace helper"]
async fn n0_real_udp_malformed_uncorrelated_duplicates() {
    fault_fixture(Candidate::N0, false).await
}

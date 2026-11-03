#![cfg(feature = "candidates")]
//! Bounded cold-client observations; not a permanent no-server guarantee.
use outpace_dht_compare::{
    candidates::{run, Candidate, Terminal},
    WorkerInput,
};
use serde_bencode::value::Value;
use std::{collections::HashMap, time::Duration};
use tokio::{
    net::UdpSocket,
    sync::oneshot,
    time::{timeout, timeout_at, Instant},
};

fn require_isolation() {
    assert_eq!(std::process::id(), 1, "requires owned PID namespace");
    let links = std::process::Command::new("/usr/bin/ip")
        .args(["-o", "link", "show"])
        .output()
        .unwrap();
    assert!(links.status.success());
    let links = String::from_utf8(links.stdout).unwrap();
    assert_eq!(links.lines().count(), 1);
    assert!(links.contains("lo:"));
    let routes = std::process::Command::new("/usr/bin/ip")
        .args(["route", "show"])
        .output()
        .unwrap();
    assert!(routes.status.success() && routes.stdout.is_empty());
}
fn packet(bytes: &[u8]) -> HashMap<Vec<u8>, Value> {
    let Value::Dict(value) = serde_bencode::from_bytes(bytes).unwrap() else {
        panic!("invalid packet")
    };
    value
}
fn response(query: &HashMap<Vec<u8>, Value>, target: bool) -> Vec<u8> {
    let mut body = HashMap::from([
        (b"id".to_vec(), Value::Bytes(vec![42; 20])),
        (b"nodes".to_vec(), Value::Bytes(Vec::new())),
    ]);
    if target {
        body.insert(
            b"values".to_vec(),
            Value::List(
                (10001u16..=10010)
                    .map(|port| {
                        let mut bytes = vec![127, 0, 0, 1];
                        bytes.extend(port.to_be_bytes());
                        Value::Bytes(bytes)
                    })
                    .collect(),
            ),
        );
        body.insert(b"token".to_vec(), Value::Bytes(b"synthetic".to_vec()));
    }
    serde_bencode::to_bytes(&Value::Dict(HashMap::from([
        (b"t".to_vec(), query.get(b"t".as_slice()).unwrap().clone()),
        (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        (b"r".to_vec(), Value::Dict(body)),
    ])))
    .unwrap()
}
fn check_query(query: &HashMap<Vec<u8>, Value>) {
    assert_eq!(
        query.get(b"ro".as_slice()),
        Some(&Value::Int(1)),
        "query lacks read-only flag"
    );
    let Some(Value::Bytes(method)) = query.get(b"q".as_slice()) else {
        panic!("missing query method")
    };
    assert!(
        matches!(method.as_slice(), b"get_peers" | b"find_node" | b"ping"),
        "unexpected query method"
    );
}
async fn fixture(candidate: Candidate) {
    require_isolation();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let seed = socket.local_addr().unwrap().to_string();
    let (started_tx, started_rx) = oneshot::channel();
    let (quiet_tx, quiet_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut bytes = [0; 65536];
        let (query, from) = loop {
            let (n, from) = socket.recv_from(&mut bytes).await.unwrap();
            let query = packet(&bytes[..n]);
            check_query(&query);
            if query.get(b"q".as_slice()) == Some(&Value::Bytes(b"get_peers".to_vec())) {
                break (query, from);
            }
            socket
                .send_to(&response(&query, false), from)
                .await
                .unwrap();
        };
        let probe_txid = Value::Bytes(vec![201, 202, 203, 204]);
        let ping = Value::Dict(HashMap::from([
            (b"t".to_vec(), probe_txid.clone()),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (
                b"a".to_vec(),
                Value::Dict(HashMap::from([(
                    b"id".to_vec(),
                    Value::Bytes(vec![42; 20]),
                )])),
            ),
        ]));
        socket
            .send_to(&serde_bencode::to_bytes(&ping).unwrap(), from)
            .await
            .unwrap();
        started_tx.send(()).unwrap();
        let until = Instant::now() + Duration::from_millis(250);
        while let Ok(received) = timeout_at(until, socket.recv_from(&mut bytes)).await {
            let (n, source) = received.unwrap();
            assert_eq!(source, from);
            let message = packet(&bytes[..n]);
            if message.get(b"y".as_slice()) == Some(&Value::Bytes(b"q".to_vec())) {
                check_query(&message);
                socket
                    .send_to(&response(&message, false), source)
                    .await
                    .unwrap();
            } else {
                assert_ne!(
                    message.get(b"t".as_slice()),
                    Some(&probe_txid),
                    "client answered unsolicited probe"
                );
            }
        }
        quiet_tx.send(()).unwrap();
        release_rx.await.unwrap();
        socket.send_to(&response(&query, true), from).await.unwrap();
    });
    let input = WorkerInput::parse("0123456789abcdef0123456789abcdef01234567", &seed).unwrap();
    let mut lookup = tokio::spawn(async move { run(candidate, &input).await });
    let mut joined = false;
    let outcome = timeout(Duration::from_secs(4), async {
        started_rx.await.ok()?;
        let active_start = !lookup.is_finished();
        quiet_rx.await.ok()?;
        let active_end = !lookup.is_finished();
        release_tx.send(()).ok()?;
        let record = (&mut lookup).await;
        joined = true;
        Some((active_start, active_end, record))
    })
    .await;
    server.abort();
    let server_result = server.await;
    if !joined {
        lookup.abort();
        let _ = lookup.await;
    }
    assert!(
        server_result.is_ok() || server_result.is_err_and(|e| e.is_cancelled()),
        "fixture failed before cleanup"
    );
    let (active_start, active_end, record) = outcome
        .expect("client-mode fixture exceeded bound")
        .expect("fixture channel closed before completion");
    assert!(
        active_start && active_end,
        "lookup not active throughout probe window"
    );
    let record = record.unwrap();
    assert_eq!(record.terminal, Terminal::Target);
    assert_eq!(record.peer_count, 10);
}
#[tokio::test]
#[ignore = "run compiled test only through offline namespace helper"]
async fn mainline_cold_client_mode() {
    fixture(Candidate::Mainline).await
}
#[tokio::test]
#[ignore = "run compiled test only through offline namespace helper"]
async fn n0_cold_client_mode() {
    fixture(Candidate::N0).await
}

// These intentionally cannot pass as ordinary test-harness runs: the external
// owner must cancel PID 1 after the real get_peers datagram witness is drained.
async fn awaiting_external_cancel(candidate: Candidate) {
    use std::io::Write;
    require_isolation();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let seed = socket.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let mut bytes = [0; 65536];
        loop {
            let (n, from) = socket.recv_from(&mut bytes).await.unwrap();
            let query = packet(&bytes[..n]);
            check_query(&query);
            if query.get(b"q".as_slice()) == Some(&Value::Bytes(b"get_peers".to_vec())) {
                println!("dht_lookup_query_witness");
                std::io::stdout().flush().unwrap();
                std::future::pending::<()>().await;
            }
            socket
                .send_to(&response(&query, false), from)
                .await
                .unwrap();
        }
    });
    let input = WorkerInput::parse("0123456789abcdef0123456789abcdef01234567", &seed).unwrap();
    let _ = run(candidate, &input).await;
    println!("dht_lookup_returned_before_cancel");
    std::io::stdout().flush().unwrap();
    server.abort();
    let _ = server.await;
    panic!("external owner did not cancel unresolved lookup");
}
#[tokio::test]
#[ignore = "requires witness-triggered external namespace cancellation"]
async fn mainline_external_lookup_cancel() {
    awaiting_external_cancel(Candidate::Mainline).await
}
#[tokio::test]
#[ignore = "requires witness-triggered external namespace cancellation"]
async fn n0_external_lookup_cancel() {
    awaiting_external_cancel(Candidate::N0).await
}

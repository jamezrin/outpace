//! Offline comparison contracts using the actual private production walker.
use super::*;

fn record_batch(history: &mut Vec<usize>, count: &mut usize, size: usize) {
    *count = count.saturating_add(1);
    if history.len() < 64 {
        history.push(size);
    }
}
#[test]
fn batch_history_remains_bounded() {
    let mut history = Vec::new();
    let mut count = 0;
    for _ in 0..65 {
        record_batch(&mut history, &mut count, 0);
    }
    assert_eq!(history, vec![0; 64]);
    assert_eq!(count, 65);
}

/// Observe the private walker without changing its response batching or peer policy.
async fn run_comparison(infohash: &[u8; 20], seeds: Vec<SocketAddrV4>) -> serde_json::Value {
    let start = std::time::Instant::now();
    let budget = Duration::from_secs(15);
    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(socket) => socket,
        Err(_) => {
            return serde_json::json!({
                "terminal": "adapter_error", "peer_count": 0,
                "batch_sizes": [], "batches_observed": 0, "batch_history_truncated": false, "target_overshoot": 0,
                "first_peer_seconds": null, "elapsed_seconds": start.elapsed().as_secs_f64(),
            })
        }
    };
    let Some(remaining) = budget
        .checked_sub(start.elapsed())
        .filter(|left| !left.is_zero())
    else {
        return serde_json::json!({
            "terminal": "deadline", "peer_count": 0,
            "batch_sizes": [], "batches_observed": 0, "batch_history_truncated": false, "target_overshoot": 0,
            "first_peer_seconds": null, "elapsed_seconds": start.elapsed().as_secs_f64(),
        });
    };
    let mut peers = BTreeSet::new();
    let mut first_peer = None;
    let mut batch_sizes = Vec::new();
    let mut batches_observed = 0;
    let metrics = dht_walk_from_seeds(infohash, remaining, &socket, seeds, |_, response| {
        record_batch(
            &mut batch_sizes,
            &mut batches_observed,
            response.peers.len(),
        );
        collect_response_peers(response, &mut peers, 8, &mut |_| {
            first_peer.get_or_insert_with(|| start.elapsed().as_secs_f64());
            true
        })
    })
    .await;
    let terminal = if peers.len() >= 8 {
        "target"
    } else if metrics.frontier_exhausted {
        "exhausted"
    } else {
        "deadline"
    };
    serde_json::json!({
        "terminal": terminal,
        "batch_sizes": batch_sizes,
        "batches_observed": batches_observed,
        "batch_history_truncated": batches_observed>batch_sizes.len(),
        "target_overshoot": peers.len().saturating_sub(8),
        "peer_count": peers.len(),
        "first_peer_seconds": first_peer,
        "elapsed_seconds": start.elapsed().as_secs_f64(),
        "nodes_queried": metrics.nodes_queried,
        "valid_responses": metrics.valid_responses,
    })
}

fn require_offline() {
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

#[tokio::test]
#[ignore = "run compiled test only through offline comparison namespace"]
async fn custom_real_udp_target() {
    require_offline();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let SocketAddr::V4(seed) = socket.local_addr().unwrap() else {
        unreachable!()
    };
    let server = tokio::spawn(async move {
        let mut packet = [0; 65536];
        let (n, from) = socket.recv_from(&mut packet).await.unwrap();
        let query = Bencode::parse(&packet[..n]).unwrap();
        assert_eq!(
            query.get(b"q").and_then(Bencode::as_bytes),
            Some(b"get_peers".as_slice())
        );
        let peers = (10001u16..=10010)
            .map(|port| {
                let mut p = vec![127, 0, 0, 1];
                p.extend(port.to_be_bytes());
                Bencode::Bytes(p)
            })
            .collect();
        let response = Bencode::Dict(BTreeMap::from([
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
        socket.send_to(&response.encode(), from).await.unwrap();
    });
    let result =
        tokio::time::timeout(Duration::from_secs(4), run_comparison(&[1; 20], vec![seed])).await;
    if !server.is_finished() {
        server.abort();
    }
    let joined = server.await;
    assert!(joined.is_ok() || joined.is_err_and(|e| e.is_cancelled()));
    let result = result.expect("custom walker missed fixture bound");
    assert_eq!(result["terminal"], "target");
    assert_eq!(result["peer_count"], 10);
    assert_eq!(result["batch_sizes"], serde_json::json!([10]));
    assert_eq!(result["batches_observed"], 1);
    assert_eq!(result["target_overshoot"], 2);
    assert_eq!(result["batch_history_truncated"], false);
    assert!(result["first_peer_seconds"].as_f64().is_some());
}

async fn custom_fault_fixture(silent: bool) {
    require_offline();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let SocketAddr::V4(seed) = socket.local_addr().unwrap() else {
        unreachable!()
    };
    let server = tokio::spawn(async move {
        let mut packet = [0; 65536];
        let (n, from) = socket.recv_from(&mut packet).await.unwrap();
        let query = Bencode::parse(&packet[..n]).unwrap();
        assert_eq!(
            query.get(b"q").and_then(Bencode::as_bytes),
            Some(b"get_peers".as_slice())
        );
        if silent {
            std::future::pending::<()>().await;
        }
        let reply = |txid, ports: Vec<u16>| {
            let peers = ports
                .into_iter()
                .map(|port| {
                    let mut p = vec![127, 0, 0, 1];
                    p.extend(port.to_be_bytes());
                    Bencode::Bytes(p)
                })
                .collect();
            Bencode::Dict(BTreeMap::from([
                (b"t".to_vec(), txid),
                (b"y".to_vec(), Bencode::Bytes(b"r".to_vec())),
                (
                    b"r".to_vec(),
                    Bencode::Dict(BTreeMap::from([
                        (b"id".to_vec(), Bencode::Bytes(vec![42; 20])),
                        (b"values".to_vec(), Bencode::List(peers)),
                    ])),
                ),
            ]))
            .encode()
        };
        socket.send_to(b"not-bencode", from).await.unwrap();
        socket
            .send_to(
                &reply(Bencode::Bytes(vec![204; 16]), (10001..=10010).collect()),
                from,
            )
            .await
            .unwrap();
        socket
            .send_to(
                &reply(query.get(b"t").unwrap().clone(), vec![10001; 10]),
                from,
            )
            .await
            .unwrap();
        // Keep the socket alive until the walker reaches its normal terminal.
        std::future::pending::<()>().await;
    });
    let result = tokio::time::timeout(
        Duration::from_secs(16),
        run_comparison(&[1; 20], vec![seed]),
    )
    .await;
    server.abort();
    assert!(
        server.await.is_err_and(|e| e.is_cancelled()),
        "fixture task failed before cleanup"
    );
    let record = result.expect("custom absolute deadline not honored");
    assert_eq!(record["terminal"], "exhausted");
    assert_eq!(record["peer_count"], usize::from(!silent));
    assert_eq!(record["first_peer_seconds"].is_null(), silent);
    assert_eq!(record["nodes_queried"], 1);
    assert_eq!(record["valid_responses"], usize::from(!silent));
}

#[tokio::test]
#[ignore = "run compiled test only through offline comparison namespace"]
async fn custom_real_udp_silent() {
    custom_fault_fixture(true).await
}
#[tokio::test]
#[ignore = "run compiled test only through offline comparison namespace"]
async fn custom_real_udp_malformed_uncorrelated_duplicates() {
    custom_fault_fixture(false).await
}

#[tokio::test]
#[ignore = "requires witness-triggered external namespace cancellation"]
async fn custom_external_lookup_cancel() {
    use std::io::Write;
    require_offline();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let SocketAddr::V4(seed) = socket.local_addr().unwrap() else {
        unreachable!()
    };
    let server = tokio::spawn(async move {
        let mut packet = [0; 65536];
        let (n, _) = socket.recv_from(&mut packet).await.unwrap();
        let query = Bencode::parse(&packet[..n]).unwrap();
        assert_eq!(
            query.get(b"q").and_then(Bencode::as_bytes),
            Some(b"get_peers".as_slice())
        );
        println!("dht_lookup_query_witness");
        std::io::stdout().flush().unwrap();
        std::future::pending::<()>().await;
    });
    let _ = run_comparison(&[1; 20], vec![seed]).await;
    println!("dht_lookup_returned_before_cancel");
    std::io::stdout().flush().unwrap();
    server.abort();
    let _ = server.await;
    panic!("external owner did not cancel unresolved lookup");
}

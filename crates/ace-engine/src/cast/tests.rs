use super::*;
use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A fake renderer: answers each SOAP POST from `replies` in order and records the
/// `SOAPACTION` headers and bodies it received.
pub(crate) async fn fake_renderer(
    replies: Vec<(u16, &'static str)>,
) -> (SocketAddrV4, tokio::task::JoinHandle<Vec<(String, String)>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let task = tokio::spawn(async move {
        let mut seen = Vec::new();
        for (status, reply) in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                request.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&request);
                if let Some(head_end) = text.find("\r\n\r\n") {
                    let length: usize = text[..head_end]
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    if request.len() >= head_end + 4 + length || n == 0 {
                        break;
                    }
                }
            }
            let text = String::from_utf8(request).unwrap();
            let action = text
                .lines()
                .find_map(|l| l.strip_prefix("SOAPACTION: "))
                .unwrap_or_default()
                .to_string();
            let body = text.split_once("\r\n\r\n").unwrap().1.to_string();
            seen.push((action, body));
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: text/xml\r\nContent-Length: {}\r\n\r\n{reply}",
                reply.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        seen
    });
    (addr, task)
}

pub(crate) const OK: &str =
    "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body/></s:Envelope>";
const FAULT: &str = include_str!("../../../../tests/vectors/dlna/soap-fault.xml");
const INFO: &str =
    include_str!("../../../../tests/vectors/dlna/soap-get-transport-info-response.xml");

fn service(bind: &str) -> CastService {
    let service = CastService::new();
    service.set_http_bind(bind.parse().unwrap());
    service
}

#[tokio::test]
async fn play_status_stop_drive_av_transport_and_track_the_stream_fetch() {
    let (addr, renderer) = fake_renderer(vec![(200, OK), (200, OK), (200, INFO), (200, OK)]).await;
    let cast = service("192.168.1.2:6878");
    cast.insert_target_for_test(test_support::target("t1", addr));

    let started = cast.play("t1", "ace", "cid:0123", "Outpace").await.unwrap();
    assert!(started.url.starts_with("http://192.168.1.2:6878/cast/s/"));
    assert!(started.url.ends_with(".ts"));
    assert_eq!(started.token.len(), 32);

    let (opened, guard) = cast.open_stream(&started.token).unwrap();
    assert_eq!(
        (opened.network.as_str(), opened.id.as_str()),
        ("ace", "cid:0123")
    );
    let status = cast.status("t1").await.unwrap();
    assert_eq!(status.transport.state, "PLAYING");
    assert_eq!(status.cast.as_ref().unwrap().connections(), 1);
    drop(guard);
    assert_eq!(status.cast.unwrap().connections(), 0);

    cast.stop("t1").await.unwrap();
    assert!(cast.open_stream(&started.token).is_none());

    let seen = renderer.await.unwrap();
    let actions: Vec<_> = seen.iter().map(|(a, _)| a.as_str()).collect();
    assert_eq!(
        actions,
        [
            "\"urn:schemas-upnp-org:service:AVTransport:1#SetAVTransportURI\"",
            "\"urn:schemas-upnp-org:service:AVTransport:1#Play\"",
            "\"urn:schemas-upnp-org:service:AVTransport:1#GetTransportInfo\"",
            "\"urn:schemas-upnp-org:service:AVTransport:1#Stop\"",
        ]
    );
    assert!(seen[0]
        .1
        .contains(&format!("<CurrentURI>{}</CurrentURI>", started.url)));
    assert!(seen[0].1.contains("http-get:*:video/mp2t:*"));
}

#[tokio::test]
async fn loopback_bind_fails_before_contacting_the_renderer() {
    let (addr, renderer) = fake_renderer(vec![]).await;
    let cast = service("127.0.0.1:6878");
    cast.insert_target_for_test(test_support::target("t1", addr));
    assert_eq!(
        cast.play("t1", "ace", "x", "t").await.unwrap_err(),
        CastError::LoopbackBind
    );
    assert!(renderer.await.unwrap().is_empty());
    let v6 = service("[::1]:6878");
    assert_eq!(v6.check_bind(), Err(CastError::LoopbackBind));
}

#[tokio::test]
async fn illegal_mime_type_falls_back_to_the_dlna_mpeg_ts_type() {
    let (addr, renderer) = fake_renderer(vec![(500, FAULT), (200, OK), (200, OK)]).await;
    let cast = service("192.168.1.2:6878");
    cast.insert_target_for_test(test_support::target("t1", addr));
    let started = cast.play("t1", "ace", "x", "t").await.unwrap();
    assert_eq!(started.mime, "video/mpeg");
    let (open, _guard) = cast.open_stream(&started.token).unwrap();
    assert_eq!(open.mime, "video/mpeg");
    let seen = renderer.await.unwrap();
    assert!(seen[0].1.contains("http-get:*:video/mp2t:*"));
    assert!(seen[1].1.contains("http-get:*:video/mpeg:*"));
    assert!(seen[2].0.ends_with("#Play\""));
}

#[tokio::test]
async fn renderer_fault_is_reported_and_the_cast_is_dropped() {
    let other_fault = FAULT
        .replace("714", "701")
        .replace("Illegal MIME-type", "Transition not available");
    let other_fault: &'static str = Box::leak(other_fault.into_boxed_str());
    let (addr, _renderer) = fake_renderer(vec![(500, other_fault)]).await;
    let cast = service("192.168.1.2:6878");
    cast.insert_target_for_test(test_support::target("t1", addr));
    let error = cast.play("t1", "ace", "x", "t").await.unwrap_err();
    assert_eq!(
        error,
        CastError::Fault(soap::Fault {
            code: Some(701),
            description: Some("Transition not available".into()),
        })
    );
    assert_eq!(error.code(), "renderer_fault");
    assert!(cast.state.lock().unwrap().casts.is_empty());
}

#[tokio::test]
async fn unknown_targets_are_rejected() {
    let cast = service("192.168.1.2:6878");
    assert_eq!(
        cast.play("missing", "ace", "x", "t").await.unwrap_err(),
        CastError::UnknownTarget
    );
    assert_eq!(
        cast.stop("missing").await.unwrap_err(),
        CastError::UnknownTarget
    );
    assert!(cast.status("missing").await.is_err());
    assert!(cast.open_stream("nope").is_none());
}

#[test]
fn stream_address_follows_the_bind() {
    let renderer = Ipv4Addr::new(192, 168, 1, 40);
    assert_eq!(
        lan_address_for("192.168.1.2".parse().unwrap(), renderer),
        Ok(Ipv4Addr::new(192, 168, 1, 2))
    );
    assert_eq!(
        lan_address_for("127.0.0.1".parse().unwrap(), renderer),
        Err(CastError::LoopbackBind)
    );
    assert_eq!(
        lan_address_for("::1".parse().unwrap(), renderer),
        Err(CastError::LoopbackBind)
    );
    assert_eq!(
        lan_address_for("fd00::2".parse().unwrap(), renderer),
        Err(CastError::UnsupportedBind)
    );
}

#[test]
fn target_ids_are_stable_and_path_safe() {
    let location = LanUrl::parse("http://192.168.1.40:49152/d.xml").unwrap();
    let renderer = description::parse(
        include_bytes!("../../../../tests/vectors/dlna/renderer-tv.xml"),
        &location,
    )
    .unwrap();
    let a = Target::from_renderer(renderer.clone(), &location);
    let b = Target::from_renderer(renderer, &location);
    assert_eq!(a.id, b.id);
    assert_eq!(a.id.len(), 16);
    assert!(a.id.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(a.address, Ipv4Addr::new(192, 168, 1, 40));
}

/// Live check against a real renderer on this LAN. Run with
/// `OUTPACE_CAST_LIVE=1 cargo test -p ace-engine cast::tests::live_discovery -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "needs a DLNA renderer on the local network"]
async fn live_discovery() {
    if std::env::var("OUTPACE_CAST_LIVE").as_deref() != Ok("1") {
        eprintln!("skipping: set OUTPACE_CAST_LIVE=1");
        return;
    }
    let cast = service("0.0.0.0:6878");
    let found = cast.targets(true).await.unwrap();
    for target in &found.targets {
        eprintln!("{} {:?} {}", target.id, target.name, target.address);
        let status = cast.status(&target.id).await;
        eprintln!("  status: {:?}", status.map(|s| s.transport));
    }
}

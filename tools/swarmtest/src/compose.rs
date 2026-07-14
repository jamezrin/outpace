//! Pure generation of a per-scenario `docker-compose.yaml`.
//!
//! [`compose_yaml`] is a deterministic function of a [`ComposeParams`] and a
//! [`Scenario`]: it lays out a dedicated bridge network with static IPs and the right
//! services (media source, swarm source, engine/outpace consumers) per the interop
//! recipes. It performs no I/O so it is exhaustively unit-tested by parsing its output
//! back with `serde_yaml`. [`crate::scenario`] writes the string into the run dir and
//! hands it to `docker compose`.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::config::Scenario;

/// Peer port a broadcasting outpace node listens on.
pub const OUTPACE_PEER_PORT: u16 = 8621;
/// Peer-wire port a real engine SOURCE node listens on by default (note 25); distinct
/// from the support-node default of 8621.
pub const ENGINE_SOURCE_PEER_PORT: u16 = 7764;
/// HTTP API port both engine and outpace serve on inside their containers.
pub const HTTP_API_PORT: u16 = 6878;
/// Port the ffmpeg media container serves/receives MPEG-TS on.
pub const MEDIA_PORT: u16 = 8090;
/// Broadcast/source name used across every scenario.
pub const BROADCAST_NAME: &str = "test";

/// Fully-resolved inputs for [`compose_yaml`]. The scenario picks the source flavour
/// and media direction; the consumer IP vectors fix how many of each kind exist.
#[derive(Debug, Clone)]
pub struct ComposeParams {
    /// Compose project name (`name:` top-level key).
    pub project_name: String,
    /// Bridge network name.
    pub network_name: String,
    /// Network subnet CIDR, e.g. `172.28.0.0/24`.
    pub subnet: String,
    /// Gateway IP the host-side tracker/httpd are reachable at, e.g. `172.28.0.1`.
    pub gateway: String,
    /// Static IP for the media (ffmpeg) container.
    pub media_ip: String,
    /// Static IP for the swarm source container.
    pub source_ip: String,
    /// Static IPs for engine consumers (length = engine consumer count).
    pub engine_consumer_ips: Vec<String>,
    /// Static IPs for outpace consumers (length = outpace consumer count).
    pub outpace_consumer_ips: Vec<String>,
    /// Image tag for the AceStream engine sandbox.
    pub engine_image: String,
    /// Image tag for the outpace build.
    pub outpace_image: String,
    /// Image reference for the ffmpeg media container (entrypoint = `ffmpeg`).
    pub ffmpeg_image: String,
    /// UDP port the host-side interop tracker listens on.
    pub tracker_port: u16,
    /// TCP port the host-side descriptor httpd listens on.
    pub httpd_port: u16,
    /// Nominal source bitrate advertised to the engine source node.
    pub bitrate: u32,
    /// Host directory bind-mounted at the engine source's `/pub` (holds `test.acelive`).
    pub source_pub_dir: String,
    /// Emit a tcpdump sidecar capturing the source container's traffic when `true`.
    pub pcap: bool,
    /// Image for the tcpdump sidecar (only used when [`Self::pcap`]).
    pub pcap_image: String,
    /// Host directory bind-mounted at the pcap sidecar's `/caps` (holds `<scenario>.pcap`).
    pub caps_dir: String,
}

impl ComposeParams {
    /// Sensible defaults for `scenario` on the fixed `172.28.0.0/24` bridge.
    ///
    /// The caller still overrides image tags, `source_pub_dir`, and the tracker/httpd
    /// ports before generating. Consumer counts follow the interop matrix: baseline has
    /// three engine consumers; mixed and outpace-source each have two engine and two
    /// outpace consumers.
    pub fn for_scenario(scenario: Scenario) -> Self {
        let (engine_count, outpace_count) = match scenario {
            Scenario::Baseline => (3, 0),
            Scenario::Mixed => (2, 2),
            Scenario::OutpaceSource => (2, 2),
        };
        let engine_consumer_ips = (0..engine_count)
            .map(|i| format!("172.28.0.{}", 21 + i))
            .collect();
        let outpace_consumer_ips = (0..outpace_count)
            .map(|i| format!("172.28.0.{}", 31 + i))
            .collect();
        Self {
            project_name: format!("swarmtest-{}", scenario.as_str()),
            network_name: "swarmnet".to_string(),
            subnet: "172.28.0.0/24".to_string(),
            gateway: "172.28.0.1".to_string(),
            media_ip: "172.28.0.10".to_string(),
            source_ip: "172.28.0.11".to_string(),
            engine_consumer_ips,
            outpace_consumer_ips,
            engine_image: "swarmtest-engine:latest".to_string(),
            outpace_image: "swarmtest-outpace:latest".to_string(),
            ffmpeg_image: "jrottenberg/ffmpeg:6.1-ubuntu".to_string(),
            tracker_port: crate::config::DEFAULT_TRACKER_ADDR
                .rsplit(':')
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(7001),
            httpd_port: crate::config::DEFAULT_HTTPD_ADDR
                .rsplit(':')
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(7002),
            bitrate: 8375,
            source_pub_dir: "./pub".to_string(),
            pcap: false,
            pcap_image: "nicolaka/netshoot".to_string(),
            caps_dir: "./caps".to_string(),
        }
    }

    /// The `udp://<gateway>:<tracker_port>/announce` URL containers announce to.
    pub fn tracker_url(&self) -> String {
        format!("udp://{}:{}/announce", self.gateway, self.tracker_port)
    }

    /// The `http://<gateway>:<httpd_port>/<scenario>.acelive` descriptor URL.
    pub fn descriptor_url(&self, scenario: Scenario) -> String {
        format!(
            "http://{}:{}/{}.acelive",
            self.gateway,
            self.httpd_port,
            scenario.as_str()
        )
    }
}

#[derive(Serialize)]
struct Compose {
    name: String,
    services: BTreeMap<String, Service>,
    networks: BTreeMap<String, NetworkDef>,
}

#[derive(Serialize, Default)]
struct Service {
    image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    environment: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    volumes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    cap_add: Vec<String>,
    /// Share another service's network namespace (mutually exclusive with `networks`).
    #[serde(skip_serializing_if = "Option::is_none")]
    network_mode: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    networks: BTreeMap<String, ServiceNet>,
    restart: String,
}

#[derive(Serialize)]
struct ServiceNet {
    ipv4_address: String,
}

#[derive(Serialize)]
struct NetworkDef {
    driver: String,
    ipam: Ipam,
}

#[derive(Serialize)]
struct Ipam {
    config: Vec<IpamConfig>,
}

#[derive(Serialize)]
struct IpamConfig {
    subnet: String,
    gateway: String,
}

/// Render the `docker-compose.yaml` for `scenario` from `p` as a YAML string.
pub fn compose_yaml(scenario: Scenario, p: &ComposeParams) -> String {
    let net = |ip: &str| {
        let mut m = BTreeMap::new();
        m.insert(
            p.network_name.clone(),
            ServiceNet {
                ipv4_address: ip.to_string(),
            },
        );
        m
    };

    let mut services: BTreeMap<String, Service> = BTreeMap::new();
    let outpace_source = matches!(scenario, Scenario::OutpaceSource);

    // --- media (ffmpeg): listens for scenarios 1-2, pushes into the outpace source for 3.
    let media_command = if outpace_source {
        ffmpeg_push_command(&p.source_ip)
    } else {
        ffmpeg_listen_command()
    };
    services.insert(
        "media".to_string(),
        Service {
            image: p.ffmpeg_image.clone(),
            command: Some(media_command),
            environment: vec![],
            volumes: vec![],
            // In push mode media must wait for the source to accept ingest.
            depends_on: if outpace_source {
                vec!["source".to_string()]
            } else {
                vec![]
            },
            networks: net(&p.media_ip),
            restart: "no".to_string(),
            ..Default::default()
        },
    );

    // --- source: real engine broadcaster (1-2) or outpace broadcaster (3).
    let source = if outpace_source {
        Service {
            image: p.outpace_image.clone(),
            command: Some(vec!["broadcast".to_string(), BROADCAST_NAME.to_string()]),
            environment: vec![
                "OUTPACE_ENABLE_INBOUND=1".to_string(),
                "OUTPACE_SEED_DEBUG=1".to_string(),
                "OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1".to_string(),
                format!("OUTPACE_TRACKERS={}", p.tracker_url()),
                format!("OUTPACE_BIND=0.0.0.0:{HTTP_API_PORT}"),
                format!("OUTPACE_PEER_LISTEN=0.0.0.0:{OUTPACE_PEER_PORT}"),
            ],
            volumes: vec![],
            depends_on: vec![],
            networks: net(&p.source_ip),
            restart: "no".to_string(),
            ..Default::default()
        }
    } else {
        Service {
            image: p.engine_image.clone(),
            command: Some(engine_source_command(&p.media_ip, &p.bitrate.to_string())),
            environment: vec![],
            volumes: vec![format!("{}:/pub", p.source_pub_dir)],
            depends_on: vec!["media".to_string()],
            networks: net(&p.source_ip),
            restart: "no".to_string(),
            ..Default::default()
        }
    };
    services.insert("source".to_string(), source);

    // --- engine consumers: default image CMD (client-console HTTP API on 6878).
    for (i, ip) in p.engine_consumer_ips.iter().enumerate() {
        services.insert(
            format!("engine-consumer-{}", i + 1),
            Service {
                image: p.engine_image.clone(),
                command: None,
                environment: vec![],
                volumes: vec![],
                depends_on: vec!["source".to_string()],
                networks: net(ip),
                restart: "no".to_string(),
                ..Default::default()
            },
        );
    }

    // --- outpace consumers: default CMD `serve`, permissive tracker policy.
    for (i, ip) in p.outpace_consumer_ips.iter().enumerate() {
        services.insert(
            format!("outpace-consumer-{}", i + 1),
            Service {
                image: p.outpace_image.clone(),
                command: None,
                environment: vec![
                    "OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1".to_string(),
                    // Fetch the patched descriptor from the harness httpd on the private
                    // bridge gateway (the SSRF guard blocks non-global hosts by default).
                    "OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT=1".to_string(),
                    format!("OUTPACE_TRACKERS={}", p.tracker_url()),
                    format!("OUTPACE_BIND=0.0.0.0:{HTTP_API_PORT}"),
                ],
                volumes: vec![],
                depends_on: vec!["source".to_string()],
                networks: net(ip),
                restart: "no".to_string(),
                ..Default::default()
            },
        );
    }

    // --- optional tcpdump sidecar sharing the SOURCE container's netns, so a capture
    // sees the source's peer-wire traffic. Only emitted when pcap capture is requested.
    if p.pcap {
        services.insert(
            "pcap".to_string(),
            Service {
                image: p.pcap_image.clone(),
                command: Some(
                    [
                        "tcpdump",
                        "-i",
                        "any",
                        "-U",
                        "-w",
                        &format!("/caps/{}.pcap", scenario.as_str()),
                    ]
                    .map(String::from)
                    .to_vec(),
                ),
                volumes: vec![format!("{}:/caps", p.caps_dir)],
                depends_on: vec!["source".to_string()],
                cap_add: vec!["NET_RAW".to_string(), "NET_ADMIN".to_string()],
                network_mode: Some("service:source".to_string()),
                restart: "no".to_string(),
                ..Default::default()
            },
        );
    }

    let mut networks = BTreeMap::new();
    networks.insert(
        p.network_name.clone(),
        NetworkDef {
            driver: "bridge".to_string(),
            ipam: Ipam {
                config: vec![IpamConfig {
                    subnet: p.subnet.clone(),
                    gateway: p.gateway.clone(),
                }],
            },
        },
    );

    let compose = Compose {
        name: p.project_name.clone(),
        services,
        networks,
    };
    serde_yaml::to_string(&compose).expect("compose model always serializes")
}

/// ffmpeg args (no leading `ffmpeg`) generating ~1.4 Mb/s H.264/AAC MPEG-TS and
/// serving it over HTTP for engine source nodes to pull.
fn ffmpeg_listen_command() -> Vec<String> {
    let mut cmd = ffmpeg_encode_args();
    cmd.extend(["-listen", "1", &format!("http://0.0.0.0:{MEDIA_PORT}/")].map(String::from));
    cmd
}

/// ffmpeg args pushing the same MPEG-TS into an outpace source via chunked HTTP PUT.
fn ffmpeg_push_command(source_ip: &str) -> Vec<String> {
    let mut cmd = ffmpeg_encode_args();
    cmd.extend(
        [
            "-method",
            "PUT",
            &format!("http://{source_ip}:{HTTP_API_PORT}/broadcast/{BROADCAST_NAME}"),
        ]
        .map(String::from),
    );
    cmd
}

/// The shared encode chain: `testsrc2` video + `sine` audio -> zerolatency fixed-GOP
/// H.264 + AAC, muxed to MPEG-TS (sink appended by the caller).
fn ffmpeg_encode_args() -> Vec<String> {
    [
        "-re",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=640x360:rate=25",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-tune",
        "zerolatency",
        "-b:v",
        "1200k",
        "-maxrate",
        "1200k",
        "-bufsize",
        "1200k",
        "-g",
        "50",
        "-keyint_min",
        "50",
        "-sc_threshold",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ar",
        "48000",
        "-f",
        "mpegts",
    ]
    .map(String::from)
    .to_vec()
}

/// `start-engine --stream-source-node` argv for a real engine broadcasting `media_ip`.
fn engine_source_command(media_ip: &str, bitrate: &str) -> Vec<String> {
    vec![
        "/app/start-engine".to_string(),
        "--stream-source-node".to_string(),
        "--source".to_string(),
        format!("http://{media_ip}:{MEDIA_PORT}/"),
        "--name".to_string(),
        BROADCAST_NAME.to_string(),
        // engine 3.2.11 requires --title in addition to --name for source nodes
        "--title".to_string(),
        BROADCAST_NAME.to_string(),
        "--bitrate".to_string(),
        bitrate.to_string(),
        "--quality".to_string(),
        "SD".to_string(),
        "--category".to_string(),
        "entertaining".to_string(),
        "--metadata-dir".to_string(),
        "/meta".to_string(),
        "--publish-dir".to_string(),
        "/pub".to_string(),
        "--cache-dir".to_string(),
        "/cache".to_string(),
        "--state-dir".to_string(),
        "/state".to_string(),
        "--log-stderr".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    fn parse(scenario: Scenario) -> Value {
        let p = ComposeParams::for_scenario(scenario);
        let yaml = compose_yaml(scenario, &p);
        serde_yaml::from_str(&yaml).expect("generated compose must be valid YAML")
    }

    fn services(v: &Value) -> &serde_yaml::Mapping {
        v.get("services").unwrap().as_mapping().unwrap()
    }

    fn service_names(v: &Value) -> Vec<String> {
        services(v)
            .keys()
            .map(|k| k.as_str().unwrap().to_string())
            .collect()
    }

    fn env_of(v: &Value, service: &str) -> Vec<String> {
        services(v)
            .get(Value::from(service))
            .and_then(|s| s.get("environment"))
            .and_then(|e| e.as_sequence())
            .map(|seq| {
                seq.iter()
                    .map(|e| e.as_str().unwrap().to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn image_of(v: &Value, service: &str) -> String {
        services(v)
            .get(Value::from(service))
            .and_then(|s| s.get("image"))
            .and_then(|i| i.as_str())
            .unwrap()
            .to_string()
    }

    fn command_of(v: &Value, service: &str) -> Vec<String> {
        services(v)
            .get(Value::from(service))
            .and_then(|s| s.get("command"))
            .and_then(|c| c.as_sequence())
            .map(|seq| {
                seq.iter()
                    .map(|e| e.as_str().unwrap().to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn ipv4_of(v: &Value, service: &str) -> String {
        let s = services(v).get(Value::from(service)).unwrap();
        let nets = s.get("networks").unwrap().as_mapping().unwrap();
        let net = nets.get(Value::from("swarmnet")).unwrap();
        net.get("ipv4_address")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn baseline_has_three_engine_consumers_and_engine_source() {
        let v = parse(Scenario::Baseline);
        let names = service_names(&v);
        assert!(names.contains(&"media".to_string()));
        assert!(names.contains(&"source".to_string()));
        assert!(names.contains(&"engine-consumer-1".to_string()));
        assert!(names.contains(&"engine-consumer-3".to_string()));
        assert!(!names.contains(&"engine-consumer-4".to_string()));
        assert!(!names.iter().any(|n| n.starts_with("outpace-consumer")));
        // Source is the real engine, driven by start-engine.
        assert_eq!(image_of(&v, "source"), "swarmtest-engine:latest");
        assert!(command_of(&v, "source").contains(&"--stream-source-node".to_string()));
        // Media serves (listen), not push.
        assert!(command_of(&v, "media").iter().any(|a| a == "-listen"));
    }

    #[test]
    fn mixed_has_two_engine_and_two_outpace_consumers() {
        let v = parse(Scenario::Mixed);
        let names = service_names(&v);
        assert!(names.contains(&"engine-consumer-2".to_string()));
        assert!(!names.contains(&"engine-consumer-3".to_string()));
        assert!(names.contains(&"outpace-consumer-1".to_string()));
        assert!(names.contains(&"outpace-consumer-2".to_string()));
        assert!(!names.contains(&"outpace-consumer-3".to_string()));
        // Real engine source, so start-engine again.
        assert_eq!(image_of(&v, "source"), "swarmtest-engine:latest");
        // Outpace consumers get the permissive tracker env pointing at the gateway.
        let env = env_of(&v, "outpace-consumer-1");
        assert!(env.contains(&"OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1".to_string()));
        assert!(env
            .iter()
            .any(|e| e == "OUTPACE_TRACKERS=udp://172.28.0.1:7001/announce"));
    }

    #[test]
    fn outpace_source_broadcasts_and_media_pushes() {
        let v = parse(Scenario::OutpaceSource);
        // Source is outpace, broadcasting.
        assert_eq!(image_of(&v, "source"), "swarmtest-outpace:latest");
        assert_eq!(
            command_of(&v, "source"),
            vec!["broadcast".to_string(), "test".to_string()]
        );
        let env = env_of(&v, "source");
        assert!(env
            .iter()
            .any(|e| e == "OUTPACE_TRACKERS=udp://172.28.0.1:7001/announce"));
        assert!(env.iter().any(|e| e.starts_with("OUTPACE_PEER_LISTEN=")));
        // Media pushes into the source via HTTP PUT.
        let media = command_of(&v, "media");
        assert!(media.iter().any(|a| a == "-method"));
        assert!(media
            .iter()
            .any(|a| a == "http://172.28.0.11:6878/broadcast/test"));
        // Both consumer kinds present.
        let names = service_names(&v);
        assert!(names.contains(&"engine-consumer-2".to_string()));
        assert!(names.contains(&"outpace-consumer-2".to_string()));
    }

    #[test]
    fn static_ips_follow_the_allocation_scheme() {
        let v = parse(Scenario::Mixed);
        assert_eq!(ipv4_of(&v, "media"), "172.28.0.10");
        assert_eq!(ipv4_of(&v, "source"), "172.28.0.11");
        assert_eq!(ipv4_of(&v, "engine-consumer-1"), "172.28.0.21");
        assert_eq!(ipv4_of(&v, "engine-consumer-2"), "172.28.0.22");
        assert_eq!(ipv4_of(&v, "outpace-consumer-1"), "172.28.0.31");
        assert_eq!(ipv4_of(&v, "outpace-consumer-2"), "172.28.0.32");
    }

    #[test]
    fn network_defines_subnet_and_gateway() {
        let v = parse(Scenario::Baseline);
        let net = v.get("networks").unwrap().get("swarmnet").unwrap();
        let cfg = net.get("ipam").unwrap().get("config").unwrap();
        let first = cfg.as_sequence().unwrap().first().unwrap();
        assert_eq!(
            first.get("subnet").unwrap().as_str().unwrap(),
            "172.28.0.0/24"
        );
        assert_eq!(
            first.get("gateway").unwrap().as_str().unwrap(),
            "172.28.0.1"
        );
    }

    #[test]
    fn pcap_sidecar_present_only_when_enabled() {
        // Disabled by default: no pcap service.
        let off = ComposeParams::for_scenario(Scenario::Baseline);
        let v: Value = serde_yaml::from_str(&compose_yaml(Scenario::Baseline, &off)).unwrap();
        assert!(!service_names(&v).contains(&"pcap".to_string()));

        // Enabled: a tcpdump sidecar sharing the source netns, capturing to /caps.
        let mut on = ComposeParams::for_scenario(Scenario::Baseline);
        on.pcap = true;
        on.caps_dir = "/tmp/run/caps".to_string();
        let v: Value = serde_yaml::from_str(&compose_yaml(Scenario::Baseline, &on)).unwrap();
        assert!(service_names(&v).contains(&"pcap".to_string()));
        let pcap = services(&v).get(Value::from("pcap")).unwrap();
        assert_eq!(
            pcap.get("network_mode").unwrap().as_str().unwrap(),
            "service:source"
        );
        let cmd = command_of(&v, "pcap");
        assert!(cmd.iter().any(|a| a == "tcpdump"));
        assert!(cmd.iter().any(|a| a == "/caps/baseline.pcap"));
        let vols = pcap.get("volumes").unwrap().as_sequence().unwrap();
        assert!(vols
            .iter()
            .any(|x| x.as_str() == Some("/tmp/run/caps:/caps")));
        let caps = pcap.get("cap_add").unwrap().as_sequence().unwrap();
        assert!(caps.iter().any(|x| x.as_str() == Some("NET_RAW")));
        // The sidecar must NOT also claim a static IP (mutually exclusive with network_mode).
        assert!(pcap.get("networks").is_none());
    }

    #[test]
    fn descriptor_and_tracker_urls_are_wellformed() {
        let p = ComposeParams::for_scenario(Scenario::Baseline);
        assert_eq!(p.tracker_url(), "udp://172.28.0.1:7001/announce");
        assert_eq!(
            p.descriptor_url(Scenario::Baseline),
            "http://172.28.0.1:7002/baseline.acelive"
        );
    }
}

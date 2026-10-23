use super::Config;

#[test]
fn config_defaults_and_serialization() {
    let default = Config::default();
    assert_eq!(default.max_inbound_per_stream, 16);
    assert_eq!(default.max_inbound_peers, 64);
    assert_eq!(default.max_unchoked, 8);

    let omitted: Config = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(omitted.max_inbound_per_stream, 16);

    for value in [0usize, 1, 16, 65] {
        let config = Config {
            max_inbound_per_stream: value,
            ..Config::default()
        };
        let serialized = serde_json::to_value(&config).unwrap();
        assert_eq!(serialized["max_inbound_per_stream"], value);
        let round_trip: Config = serde_json::from_value(serialized).unwrap();
        assert_eq!(round_trip.max_inbound_per_stream, value);
    }

    for invalid in [
        serde_json::json!({"max_inbound_per_stream": "16"}),
        serde_json::json!({"max_inbound_per_stream": -1}),
    ] {
        assert!(serde_json::from_value::<Config>(invalid).is_err());
    }

    assert!(
        serde_json::from_str::<Config>(r#"{"max_inbound_per_stream":18446744073709551616}"#)
            .is_err()
    );
}

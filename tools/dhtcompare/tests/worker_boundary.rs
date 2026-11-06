#![cfg(feature = "workers")]
use outpace_dht_compare::{
    candidates::{Record, Terminal},
    worker::{encode_record, parse_environment, parse_role_args, Role},
};
use std::{ffi::OsString, time::Duration};
const TARGET: &str = "0123456789abcdef0123456789abcdef01234567";
fn environment() -> Vec<(OsString, OsString)> {
    [
        ("MODE", "offline-v1"),
        ("ROLE", "mainline"),
        ("TARGET", TARGET),
        ("SEEDS", "127.0.0.1:9001"),
    ]
    .into_iter()
    .map(|(k, v)| {
        (
            OsString::from(format!("OUTPACE_DHT_COMPARE_{k}")),
            OsString::from(v),
        )
    })
    .collect()
}
#[test]
fn fixed_arguments_and_input_are_redacted() {
    let args = ["--offline-worker", "mainline"].map(OsString::from);
    let role = parse_role_args(args).unwrap();
    assert_eq!(role, Role::Mainline);
    let request = parse_environment(role, environment()).unwrap();
    assert!(!format!("{request:?}").contains(TARGET));
    assert!(!format!("{request:?}").contains("127.0.0.1"));
    for args in [
        vec![],
        vec!["mainline"],
        vec!["--live-worker", "mainline"],
        vec!["--offline-worker", "custom"],
        vec!["--offline-worker", "n0", "extra"],
    ] {
        assert_eq!(
            parse_role_args(args.into_iter().map(OsString::from))
                .unwrap_err()
                .token(),
            "input_error"
        );
    }
}
#[test]
fn environment_rejects_missing_duplicate_unknown_and_nonloopback() {
    for (key, value) in [
        ("MODE", "live-v1"),
        ("ROLE", "n0"),
        ("TARGET", "sensitive-invalid"),
        ("SEEDS", "192.0.2.1:9001"),
        ("SEEDS", "127.0.0.1:0"),
        ("SEEDS", "127.0.0.1:1,127.0.0.1:1"),
    ] {
        let mut entries = environment();
        entries
            .iter_mut()
            .find(|(k, _)| k == &OsString::from(format!("OUTPACE_DHT_COMPARE_{key}")))
            .unwrap()
            .1 = OsString::from(value);
        assert_eq!(
            parse_environment(Role::Mainline, entries)
                .unwrap_err()
                .token(),
            "input_error"
        );
    }
    let mut missing = environment();
    missing.pop();
    assert!(parse_environment(Role::Mainline, missing).is_err());
    let mut duplicate = environment();
    duplicate.push(duplicate[0].clone());
    assert!(parse_environment(Role::Mainline, duplicate).is_err());
    let mut unknown = environment();
    unknown.push(("OUTPACE_DHT_COMPARE_OVERRIDE".into(), "1".into()));
    assert!(parse_environment(Role::Mainline, unknown).is_err());
}
#[test]
fn one_bounded_numeric_record_has_no_input() {
    let record = Record {
        peer_count: 10,
        first_peer: Some(Duration::from_millis(5)),
        elapsed: Duration::from_millis(8),
        terminal: Terminal::Target,
        batch_sizes: vec![10],
        batches_observed: 1,
        batch_history_truncated: false,
        target_overshoot: 2,
    };
    let bytes = encode_record(Role::Mainline, &record).unwrap();
    assert!(bytes.len() <= 8193 && bytes.ends_with(b"\n"));
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("\"schema_version\":1") && text.contains("\"candidate\":\"mainline\""));
    assert!(text.contains("\"peer_count\":10"));
    assert!(!text.contains(TARGET) && !text.contains("127.0.0.1"));
}

#[test]
fn inconsistent_batch_accounting_is_not_emitted() {
    let record = Record {
        peer_count: 10,
        first_peer: Some(Duration::from_millis(5)),
        elapsed: Duration::from_millis(8),
        terminal: Terminal::Target,
        batch_sizes: vec![0],
        batches_observed: 1,
        batch_history_truncated: false,
        target_overshoot: 2,
    };
    assert_eq!(
        encode_record(Role::Mainline, &record).unwrap_err().token(),
        "output_error"
    );
}

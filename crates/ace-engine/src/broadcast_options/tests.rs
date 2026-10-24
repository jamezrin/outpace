//! Tests for the strict, presence-preserving broadcast profile schema.

use crate::broadcast_options::{
    parse_broadcast_options, validate_broadcast_options, BroadcastOptions,
};

#[test]
fn options51_schema_presence_and_limits() {
    assert!(parse_broadcast_options(None).unwrap().is_empty());
    assert!(parse_broadcast_options(Some("{}")).unwrap().is_empty());

    let parsed = parse_broadcast_options(Some(r#"{"synthetic-origin":{}}"#)).unwrap();
    let empty = &parsed["synthetic-origin"];
    assert!(empty.title.is_none());
    assert!(empty.bitrate_bytes_per_sec.is_none());
    assert!(empty.piece_length.is_none());
    assert!(empty.quality.is_none());
    assert!(empty.categories.is_none());
    assert_eq!(serde_json::to_string(empty).unwrap(), "{}");

    let explicit = parse_broadcast_options(Some(
        r#"{"synthetic-origin":{"title":"synthetic-origin","bitrate_bytes_per_sec":8375,"piece_length":65536,"quality":"auto","categories":["other"]}}"#,
    )).unwrap();
    let explicit = &explicit["synthetic-origin"];
    assert_eq!(explicit.title.as_deref(), Some("synthetic-origin"));
    assert_eq!(explicit.bitrate_bytes_per_sec, Some(8375));
    assert_eq!(explicit.piece_length, Some(65536));
    assert_eq!(explicit.quality.as_deref(), Some("auto"));
    assert_eq!(
        explicit.categories.as_deref(),
        Some(["other".to_owned()].as_slice())
    );
    assert!(serde_json::to_string(explicit)
        .unwrap()
        .contains("\"piece_length\":65536"));
    assert_eq!(
        explicit
            .resolve_fresh("synthetic-origin")
            .unwrap()
            .piece_length,
        65536
    );

    for field in [
        "title",
        "bitrate_bytes_per_sec",
        "piece_length",
        "quality",
        "categories",
    ] {
        let raw = format!(r#"{{"synthetic-origin":{{"{field}":null}}}}"#);
        assert_eq!(
            parse_broadcast_options(Some(&raw)).unwrap_err().to_string(),
            "invalid_broadcast_options"
        );
    }
    for raw in [
        r#"{"synthetic-origin":{"title":"x","\u0074itle":"y"}}"#,
        r#"{"synthetic-origin":{"chunk_length":16384}}"#,
        r#"{"synthetic-origin":{"unknown":1}}"#,
        r#"{"synthetic-origin":{},"\u0073ynthetic-origin":{}}"#,
        r#"{"synthetic-origin":{}} trailing"#,
        "[]",
        "null",
        "   ",
    ] {
        assert_eq!(
            parse_broadcast_options(Some(raw)).unwrap_err().to_string(),
            "invalid_broadcast_options",
            "{raw}"
        );
    }
    assert_eq!(
        parse_broadcast_options(Some("{secret-value}"))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
    assert!(!format!("{:?}", explicit).contains("synthetic-origin"));

    let resolved = parse_broadcast_options(Some(
        r#"{"synthetic-origin":{"title":"é","bitrate_bytes_per_sec":2305843009213693951,"piece_length":131072,"quality":"HD","categories":[]}}"#,
    )).unwrap()["synthetic-origin"].resolve_fresh("synthetic-origin").unwrap();
    assert_eq!(resolved.piece_length, 131072);
    assert_eq!(resolved.bitrate_bytes_per_sec, 2305843009213693951);
    assert!(resolved.categories.is_empty());
    let min_numeric = parse_broadcast_options(Some(
        r#"{"synthetic-origin":{"bitrate_bytes_per_sec":1,"piece_length":65536}}"#,
    ))
    .unwrap();
    assert_eq!(
        min_numeric["synthetic-origin"]
            .resolve_fresh("synthetic-origin")
            .unwrap()
            .bitrate_bytes_per_sec,
        1
    );
    let max_numeric =
        parse_broadcast_options(Some(r#"{"synthetic-origin":{"piece_length":1048576}}"#)).unwrap();
    assert_eq!(
        max_numeric["synthetic-origin"]
            .resolve_fresh("synthetic-origin")
            .unwrap()
            .piece_length,
        1_048_576
    );

    for raw in [
        r#"{"synthetic-origin":{"bitrate_bytes_per_sec":0}}"#,
        r#"{"synthetic-origin":{"bitrate_bytes_per_sec":2305843009213693952}}"#,
        r#"{"synthetic-origin":{"bitrate_bytes_per_sec":1.0}}"#,
        r#"{"synthetic-origin":{"bitrate_bytes_per_sec":1e3}}"#,
        r#"{"synthetic-origin":{"bitrate_bytes_per_sec":-1}}"#,
        r#"{"synthetic-origin":{"piece_length":0}}"#,
        r#"{"synthetic-origin":{"piece_length":65535}}"#,
        r#"{"synthetic-origin":{"piece_length":1048577}}"#,
        r#"{"synthetic-origin":{"piece_length":98304}}"#,
        r#"{"synthetic-origin":{"quality":"SD "}}"#,
        r#"{"synthetic-origin":{"quality":"ULTRA"}}"#,
        r#"{"synthetic-origin":{"title":" leading"}}"#,
        r#"{"synthetic-origin":{"title":"trailing "}}"#,
        r#"{"synthetic-origin":{"title":""}}"#,
        r#"{"synthetic-origin":{"title":"line\nbreak"}}"#,
        r#"{"synthetic-origin":{"title":"control\u0001character"}}"#,
        r#"{"synthetic-origin":{"categories":["other","other"]}}"#,
        r#"{"synthetic-origin":{"categories":[""]}}"#,
    ] {
        assert_eq!(
            parse_broadcast_options(Some(raw)).unwrap_err().to_string(),
            "invalid_broadcast_options",
            "{raw}"
        );
    }
    for name in ["", ".", "..", "has space", "é", "a/b"] {
        let raw = format!("{{{:?}:{{}}}}", name);
        assert_eq!(
            parse_broadcast_options(Some(&raw)).unwrap_err().to_string(),
            "invalid_broadcast_options"
        );
    }

    let mut max_title = "x".repeat(256);
    let max_raw = format!(
        r#"{{"synthetic-origin":{{"title":{}}}}}"#,
        serde_json::to_string(&max_title).unwrap()
    );
    assert!(parse_broadcast_options(Some(&max_raw)).is_ok());
    max_title.push('x');
    let too_long = format!(
        r#"{{"synthetic-origin":{{"title":{}}}}}"#,
        serde_json::to_string(&max_title).unwrap()
    );
    assert_eq!(
        parse_broadcast_options(Some(&too_long))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
    let utf8_256 = "é".repeat(128);
    let utf8_ok = format!(
        r#"{{"synthetic-origin":{{"title":{}}}}}"#,
        serde_json::to_string(&utf8_256).unwrap()
    );
    assert!(parse_broadcast_options(Some(&utf8_ok)).is_ok());
    let utf8_258 = "é".repeat(129);
    let utf8_too_long = format!(
        r#"{{"synthetic-origin":{{"title":{}}}}}"#,
        serde_json::to_string(&utf8_258).unwrap()
    );
    assert_eq!(
        parse_broadcast_options(Some(&utf8_too_long))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
    let label = "x".repeat(128);
    let labels = (0..32)
        .map(|i| format!("{i:02}{}", &label[2..]))
        .collect::<Vec<_>>();
    let cat = format!(
        r#"{{"synthetic-origin":{{"categories":{}}}}}"#,
        serde_json::to_string(&labels).unwrap()
    );
    assert!(parse_broadcast_options(Some(&cat)).is_ok());
    let too_many_categories = format!(
        r#"{{"synthetic-origin":{{"categories":{}}}}}"#,
        serde_json::to_string(&(0..33).map(|i| format!("category-{i}")).collect::<Vec<_>>())
            .unwrap()
    );
    assert_eq!(
        parse_broadcast_options(Some(&too_many_categories))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
    let max_name = "n".repeat(64);
    assert!(parse_broadcast_options(Some(&format!("{{{max_name:?}:{{}}}}"))).is_ok());
    assert_eq!(
        parse_broadcast_options(Some(&format!("{{{:?}:{{}}}}", "n".repeat(65))))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );

    let mut many = String::from("{");
    for i in 0..64 {
        if i != 0 {
            many.push(',');
        }
        many.push_str(&format!("\"profile-{i:02}\":{{}}"));
    }
    many.push('}');
    assert_eq!(parse_broadcast_options(Some(&many)).unwrap().len(), 64);
    many.pop();
    many.push_str(",\"profile-extra\":{}}");
    assert_eq!(
        parse_broadcast_options(Some(&many))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
    let oversized = format!("{{}}{}", " ".repeat(262_145));
    assert_eq!(
        parse_broadcast_options(Some(&oversized))
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
    let max_input = format!("{}{{}}", " ".repeat(262_142));
    assert!(parse_broadcast_options(Some(&max_input)).is_ok());

    let mut direct = std::collections::BTreeMap::new();
    direct.insert(
        "synthetic-origin".to_owned(),
        BroadcastOptions {
            title: Some("x".to_owned()),
            ..Default::default()
        },
    );
    assert!(validate_broadcast_options(&direct).is_ok());
    direct.insert("bad name".to_owned(), BroadcastOptions::default());
    assert_eq!(
        validate_broadcast_options(&direct).unwrap_err().to_string(),
        "invalid_broadcast_options"
    );

    let mut over_compact = std::collections::BTreeMap::new();
    for profile_index in 0..64 {
        let categories = (0..32)
            .map(|category_index| {
                format!(
                    "p{profile_index:02}-c{category_index:02}-{}",
                    "x".repeat(120)
                )
            })
            .collect::<Vec<_>>();
        over_compact.insert(
            format!("profile-{profile_index:02}"),
            BroadcastOptions {
                categories: Some(categories),
                ..Default::default()
            },
        );
    }
    assert_eq!(
        validate_broadcast_options(&over_compact)
            .unwrap_err()
            .to_string(),
        "invalid_broadcast_options"
    );
}

#[test]
fn options51_public_serde_errors_redact_profile_values() {
    const MARKER: &str = "SYNTHETIC_SECRET_MARKER";

    let cases = [
        (
            "BroadcastOptions categories wrong type",
            serde_json::from_str::<BroadcastOptions>(
                r#"{"categories":"SYNTHETIC_SECRET_MARKER"}"#,
            )
            .map(|_| ()),
        ),
        (
            "BroadcastOptions numeric wrong type",
            serde_json::from_str::<BroadcastOptions>(
                r#"{"bitrate_bytes_per_sec":"SYNTHETIC_SECRET_MARKER"}"#,
            )
            .map(|_| ()),
        ),
        (
            "BroadcastOptions root marker",
            serde_json::from_str::<BroadcastOptions>(r#""SYNTHETIC_SECRET_MARKER""#)
                .map(|_| ()),
        ),
        (
            "Config categories wrong type",
            serde_json::from_str::<crate::config::Config>(
                r#"{"broadcast_options":{"synthetic-origin":{"categories":"SYNTHETIC_SECRET_MARKER"}}}"#,
            )
            .map(|_| ()),
        ),
        (
            "Config numeric wrong type",
            serde_json::from_str::<crate::config::Config>(
                r#"{"broadcast_options":{"synthetic-origin":{"bitrate_bytes_per_sec":"SYNTHETIC_SECRET_MARKER"}}}"#,
            )
            .map(|_| ()),
        ),
        (
            "Config profile-root marker",
            serde_json::from_str::<crate::config::Config>(
                r#"{"broadcast_options":"SYNTHETIC_SECRET_MARKER"}"#,
            )
            .map(|_| ()),
        ),
        (
            "Config root marker",
            serde_json::from_str::<crate::config::Config>(r#""SYNTHETIC_SECRET_MARKER""#)
                .map(|_| ()),
        ),
    ];

    let violations = cases
        .into_iter()
        .filter_map(|(label, result)| match result {
            Ok(()) => Some(format!("{label}: malformed input unexpectedly accepted")),
            Err(error) => {
                let diagnostic = error.to_string();
                (!diagnostic.contains("invalid_broadcast_options") || diagnostic.contains(MARKER))
                    .then(|| format!("{label}: {diagnostic}"))
            }
        })
        .collect::<Vec<_>>();
    assert!(
        violations.is_empty(),
        "public serde diagnostics must be fixed and redact values: {violations:?}"
    );
}

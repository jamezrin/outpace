use super::*;
use serde_json::json;

// Independently generated with Python hashlib and the recovered protocol constants.
// The request vector includes the advertised Outpace 1.1.0 version and numeric code.
// Changing canonicalization, encoding, nonce binding, keys or compact aliases breaks these.
const NONCE: &str = "123456789";
const DATA: &str = "eyJ0b3RhbCI6MSwidGltZSI6MC4wMSwicmVzdWx0cyI6W3sibmFtZSI6IlN5bnRoZXRpYyBHcm91cCIsIml0ZW1zIjpbeyJuIjoiU3ludGhldGljIEl0ZW0iLCJpIjoiMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWYwMTIzNDU2NyIsImIiOjEwMDAsImMiOlsidHYiXSwiYSI6MC41LCJhdSI6MCwicyI6MiwiZCI6ZmFsc2UsIl9kYXRhIjoiZGlzY2FyZCIsInUiOiJodHRwOi8vZXhhbXBsZS5pbnZhbGlkLyIsImNvbnRlbnRfaWQiOiIwMTIzNDU2Nzg5YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3In1dfV19";
const SIGN: [u8; 20] = [
    131, 88, 252, 122, 128, 215, 98, 179, 68, 204, 10, 197, 178, 78, 72, 155, 174, 189, 128, 173,
];

fn envelope(data: &str, sign: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&json!({"data":data,"sign":hex::encode(sign)})).unwrap()
}

#[test]
fn native_request_matches_independent_vector_before_form_encoding() {
    let query = SearchQuery::parse(vec![
        ("query".into(), "a&key=+é".into()),
        ("page".into(), "2".into()),
        ("category".into(), "tv & radio".into()),
    ])
    .unwrap();
    let request = NativeRequest::new(&query, 1_700_000_000, NONCE.into());
    let url = request.url("s5.torrentstream.net");
    assert!(url.starts_with("http://s5.torrentstream.net/a?"));
    let params: std::collections::BTreeMap<_, _> =
        form_urlencoded::parse(url.split_once('?').unwrap().1.as_bytes())
            .into_owned()
            .collect();
    assert_eq!(
        params["_s"],
        hex::encode([
            70, 69, 164, 60, 29, 112, 39, 225, 203, 188, 137, 237, 162, 41, 207, 127, 207, 85, 50,
            46
        ])
    );
    assert_eq!(params["a"], "a&key=+é");
    assert_eq!(params["c"], "tv & radio");
    assert_eq!(params["m"], "4");
    assert_eq!(params["_p"], "outpace");
    assert_eq!(params["_n"], env!("CARGO_PKG_VERSION"));
    assert_eq!(params.len(), 13);
    assert!(url.contains("a=a%26key%3D%2B%C3%A9"));
}

#[test]
fn signed_native_response_expands_compact_fields_and_strips_untrusted_extras() {
    let bytes = decode_response(&envelope(DATA, &SIGN), NONCE).unwrap();
    let query = SearchQuery::parse(vec![]).unwrap();
    let result = super::super::normalize_response(&bytes, &query).unwrap();
    let item = &result["result"]["results"][0]["items"][0];
    assert_eq!(
        item,
        &json!({
            "name":"Synthetic Item", "infohash":"0123456789abcdef0123456789abcdef01234567",
            "disabled":false,"bitrate":1000,"categories":["tv"],"availability":0.5,"availability_updated_at":0,"status":2
        })
    );
    assert_eq!(result["result"]["total"], 1);
}

#[test]
fn native_response_requires_nonce_bound_signature_before_decoding() {
    assert_eq!(
        decode_response(&envelope(DATA, &[0; 20]), NONCE),
        Err(SearchError::InvalidResponse)
    );
    assert_eq!(
        decode_response(&envelope(DATA, &SIGN), "123456788"),
        Err(SearchError::InvalidResponse)
    );
    for bytes in [
        b"{}".as_slice(),
        b"not JSON",
        br#"{"data":"!","sign":null}"#,
    ] {
        assert_eq!(
            decode_response(bytes, NONCE),
            Err(SearchError::InvalidResponse)
        );
    }
    assert_eq!(
        decode_response(&vec![b' '; MAX_RESPONSE_BYTES + 1], NONCE),
        Err(SearchError::TooLarge)
    );
}

fn signed_data(data: &str) -> Vec<u8> {
    let mut hash = Sha1::new();
    hash.update(data.as_bytes());
    hash.update(NONCE.as_bytes());
    hash.update(RESPONSE_KEY);
    envelope(data, &hash.finalize())
}

#[test]
fn correctly_signed_invalid_base64_json_or_schema_still_fails() {
    for data in ["!", "bm90IEpTT04=", "e30="] {
        assert_eq!(
            decode_response(&signed_data(data), NONCE),
            Err(SearchError::InvalidResponse)
        );
    }
    let page = json!({"total":1,"time":0.0,"results":[{"name":"Synthetic Group","items":[{"n":"Synthetic Item","u":"http://example.invalid/"}]}]});
    let raw = serde_json::to_vec(&page).unwrap();
    let expanded = decode_response(&signed_data(&Base64::encode_string(&raw)), NONCE).unwrap();
    assert_eq!(
        super::super::normalize_response(&expanded, &SearchQuery::parse(vec![]).unwrap()),
        Err(SearchError::InvalidResponse)
    );
}

#[test]
fn signed_compact_metadata_is_checked_after_expansion() {
    let page = json!({"total":1,"time":0.0,"results":[{"name":"Synthetic Group","items":[{
        "n":"Synthetic Item","i":"0123456789abcdef0123456789abcdef01234567",
        "countries":["zz"],"languages":["zz"],"d":1
    }]}]});
    let raw = serde_json::to_vec(&page).unwrap();
    let expanded = decode_response(&signed_data(&Base64::encode_string(&raw)), NONCE).unwrap();
    let result =
        super::super::normalize_response(&expanded, &SearchQuery::parse(vec![]).unwrap()).unwrap();
    let item = &result["result"]["results"][0]["items"][0];
    assert_eq!(item["countries"], json!(["zz"]));
    assert_eq!(item["languages"], json!(["zz"]));
    assert_eq!(item["disabled"], true);
}

#[test]
fn native_disabled_flag_accepts_binary_numbers_and_rejects_other_values() {
    for (flag, expected) in [
        (json!(0), Some(false)),
        (json!(1), Some(true)),
        (json!(false), Some(false)),
        (json!(true), Some(true)),
        (json!(2), None),
        (json!(-1), None),
        (json!(null), None),
        (json!("1"), None),
    ] {
        let page = json!({"total":1,"time":0.0,"results":[{"name":"Synthetic Group","items":[{
            "n":"Synthetic Item","i":"0123456789abcdef0123456789abcdef01234567","d":flag
        }]}]});
        let response = decode_response(
            &signed_data(&Base64::encode_string(&serde_json::to_vec(&page).unwrap())),
            NONCE,
        );
        match expected {
            Some(expected) => {
                let normalized = super::super::normalize_response(
                    &response.unwrap(),
                    &SearchQuery::parse(vec![]).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    normalized["result"]["results"][0]["items"][0]["disabled"],
                    expected
                );
            }
            None => assert_eq!(response, Err(SearchError::InvalidResponse)),
        }
    }
}

fn synthetic_identity() -> ace_swarm::engine_profile::EngineIdentity {
    ace_swarm::engine_profile::EngineIdentity::new(
        ("3.2.11".into(), 3_021_100),
        ace_swarm::engine_profile::Platform::Linux,
        [0x11; 20],
        [0x22; 20],
        String::new(),
    )
}

fn query_of(pairs: &[(&str, &str)]) -> SearchQuery {
    SearchQuery::parse(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
    .unwrap()
}

// Signatures generated independently with Python hashlib over the engine's canonical
// form; the same scheme reproduces a captured official 3.2.11 request.
#[test]
fn engine_shaped_request_matches_independent_vector_and_order() {
    let identity = synthetic_identity();
    let query = query_of(&[("query", "news"), ("page_size", "5")]);
    let request = NativeRequest::for_client(&query, 1_700_000_000, NONCE.into(), Some(&identity));
    let path = request.path_and_query();
    let keys: Vec<String> = form_urlencoded::parse(path.split_once('?').unwrap().1.as_bytes())
        .map(|(k, _)| k.into_owned())
        .collect();
    assert_eq!(
        keys,
        ["_a", "_b", "_n", "_p", "_r", "_t", "_v", "a", "i", "m", "_s"]
    );
    let params: std::collections::BTreeMap<_, _> =
        form_urlencoded::parse(path.split_once('?').unwrap().1.as_bytes())
            .into_owned()
            .collect();
    assert_eq!(params["_a"], hex::encode([0x11; 20]));
    assert_eq!(params["_b"], hex::encode([0x22; 20]));
    assert_eq!(params["_p"], "linux");
    assert_eq!(
        params["_s"],
        hex::encode([
            101, 213, 67, 190, 44, 176, 229, 156, 210, 7, 253, 70, 113, 14, 176, 172, 32, 244, 54,
            15
        ])
    );

    let query = query_of(&[
        ("query", "news"),
        ("page_size", "5"),
        ("page", "1"),
        ("category", "sport"),
    ]);
    let request = NativeRequest::for_client(&query, 1_700_000_000, NONCE.into(), Some(&identity));
    let params: std::collections::BTreeMap<_, _> = form_urlencoded::parse(
        request
            .path_and_query()
            .split_once('?')
            .unwrap()
            .1
            .as_bytes(),
    )
    .into_owned()
    .collect();
    assert_eq!((params["h"].as_str(), params["c"].as_str()), ("1", "sport"));
    assert!(!params.contains_key("s") && !params.contains_key("t"));
    assert_eq!(
        params["_s"],
        hex::encode([
            179, 212, 182, 8, 129, 49, 91, 253, 173, 30, 163, 47, 124, 154, 216, 9, 46, 24, 179,
            175
        ])
    );
}

#[test]
fn outpace_request_is_unchanged_without_identity() {
    let query = query_of(&[("query", "news")]);
    let request = NativeRequest::for_client(&query, 1, NONCE.into(), None);
    assert!(request.path_and_query().contains("_p=outpace"));
}

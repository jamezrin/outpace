use super::*;
use serde_json::json;

const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

fn params(values: &[(&str, &str)]) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect()
}
fn query() -> SearchQuery {
    SearchQuery {
        query: String::new(),
        page: 0,
        page_size: 10,
        category: None,
    }
}
fn group() -> Value {
    json!({"name":"Synthetic Channel", "items":[{"infohash":HASH,"name":"Synthetic Stream","categories":["tv"],"availability":0.5,"bitrate":1234,"status":2}]})
}
fn adapt(value: Value) -> Result<Value, SearchError> {
    normalize_response(&serde_json::to_vec(&value).unwrap(), &query())
}

#[test]
fn query_defaults_and_encoded_text_are_preserved() {
    assert_eq!(SearchQuery::parse(vec![]).unwrap(), query());
    let parsed = SearchQuery::parse(params(&[
        ("query", "sí & +?"),
        ("page", "1"),
        ("page_size", "200"),
        ("category", "tv"),
    ]))
    .unwrap();
    assert_eq!(parsed.query, "sí & +?");
    assert_eq!((parsed.page, parsed.page_size), (1, 200));
    assert_eq!(parsed.category.as_deref(), Some("tv"));
}

#[test]
fn invalid_query_is_rejected_before_network_work() {
    for values in [
        vec![("page", "-1")],
        vec![("page", "10001")],
        vec![("page_size", "0")],
        vec![("page_size", "201")],
        vec![("page", "x")],
        vec![("query", "a"), ("query", "b")],
        vec![("url", "https://example.invalid")],
        vec![("category", "bad\nvalue")],
    ] {
        assert_eq!(
            SearchQuery::parse(params(&values)),
            Err(SearchError::InvalidQuery)
        );
    }
    assert_eq!(
        SearchQuery::parse(vec![("query".into(), "é".repeat(129))]),
        Err(SearchError::InvalidQuery)
    );
    assert_eq!(
        SearchQuery::parse(vec![("category".into(), "a".repeat(65))]),
        Err(SearchError::InvalidQuery)
    );
}

#[test]
fn current_and_historical_envelopes_normalize_to_the_same_contract() {
    let page = json!({"total":1,"results":[group()],"time":0.01,"groupped":true});
    let flat = adapt(page.clone()).unwrap();
    assert_eq!(flat, adapt(json!({"result":page,"error":null})).unwrap());
    assert_eq!(flat["result"]["results"][0]["items"][0]["infohash"], HASH);
    assert_eq!(flat["result"]["total"], 1);
    assert!(flat["error"].is_null());
    assert!(flat["result"].get("time").is_none());
}

#[test]
fn empty_results_are_success_and_failures_are_not_empty_success() {
    assert_eq!(
        adapt(json!({"total":0,"results":[]})).unwrap(),
        json!({"result":{"total":0,"results":[]},"error":null})
    );
    assert_eq!(
        adapt(json!({"result":null,"error":"private upstream message"})),
        Err(SearchError::Upstream)
    );
    for value in [
        json!({}),
        json!({"total":1,"results":{}}),
        json!({"total":-1,"results":[]}),
        json!({"total":1,"results":[{"name":"Synthetic Category","category":"tv"}]}),
    ] {
        assert_eq!(adapt(value), Err(SearchError::InvalidResponse));
    }
    assert_eq!(
        normalize_response(b"not JSON", &query()),
        Err(SearchError::InvalidResponse)
    );
}

#[test]
fn untrusted_metadata_is_bounded_and_not_promoted_to_content_ids() {
    let mut item = group();
    item["items"][0]["content_id"] = json!(HASH);
    item["items"][0]["private_field"] = json!("not forwarded");
    let output = adapt(json!({"total":1,"results":[item]})).unwrap();
    let normalized = &output["result"]["results"][0]["items"][0];
    assert!(normalized.get("content_id").is_none());
    assert!(normalized.get("private_field").is_none());
    for invalid in [json!("wrong"), json!(42), json!(null)] {
        let mut item = group();
        item["items"][0]["infohash"] = invalid;
        assert_eq!(
            adapt(json!({"total":1,"results":[item]})),
            Err(SearchError::InvalidResponse)
        );
    }
    let mut item = group();
    item["name"] = json!("a".repeat(257));
    assert_eq!(
        adapt(json!({"total":1,"results":[item]})),
        Err(SearchError::InvalidResponse)
    );
    let mut item = group();
    item["items"][0]["availability"] = json!(1.1);
    assert_eq!(
        adapt(json!({"total":1,"results":[item]})),
        Err(SearchError::InvalidResponse)
    );
}

#[test]
fn body_group_and_item_counts_have_hard_limits() {
    assert_eq!(
        normalize_response(&vec![b' '; 1_048_577], &query()),
        Err(SearchError::TooLarge)
    );
    assert_eq!(
        adapt(json!({"total":11,"results":vec![group();11]})),
        Err(SearchError::InvalidResponse)
    );
    let mut item = group();
    item["items"] = json!(vec![item["items"][0].clone(); 201]);
    assert_eq!(
        adapt(json!({"total":1,"results":[item]})),
        Err(SearchError::InvalidResponse)
    );
}

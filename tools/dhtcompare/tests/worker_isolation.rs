#![cfg(feature = "workers")]
use outpace_dht_compare::worker::validate_snapshot;
use serde_json::{json, Value};

fn snapshot() -> [Value; 4] {
    [
        json!([{"ifindex":1,"ifname":"lo","flags":["LOOPBACK","UP","LOWER_UP"],"link_type":"loopback"}]),
        json!([{"ifindex":1,"ifname":"lo","addr_info":[
            {"family":"inet","local":"127.0.0.1","prefixlen":8,"scope":"host"},
            {"family":"inet6","local":"::1","prefixlen":128,"scope":"host"}
        ]}]),
        json!([
            {"type":"local","dst":"127.0.0.0/8","dev":"lo","table":"local","protocol":"kernel","scope":"host","prefsrc":"127.0.0.1","flags":[]},
            {"type":"broadcast","dst":"127.255.255.255","dev":"lo","table":"local","protocol":"kernel","scope":"link","prefsrc":"127.0.0.1","flags":[]}
        ]),
        json!([{"type":"local","dst":"::1","dev":"lo","table":"local","protocol":"kernel","metric":0,"flags":[],"pref":"medium"}]),
    ]
}
fn allowed(values: &[Value; 4]) -> bool {
    let bytes = values.each_ref().map(|v| serde_json::to_vec(v).unwrap());
    validate_snapshot(bytes.each_ref().map(Vec::as_slice)).is_ok()
}
#[test]
fn allows_only_up_loopback_and_both_family_local_routes() {
    assert!(allowed(&snapshot()));
    let mut s = snapshot();
    s[3] = json!([]);
    assert!(allowed(&s));
}
#[test]
fn rejects_external_links_addresses_and_routes_in_any_table() {
    for field in ["ifname", "link_type", "flags"] {
        let mut s = snapshot();
        s[0][0][field] = match field {
            "flags" => json!(["LOOPBACK"]),
            "ifname" => json!("eth0"),
            _ => json!("ether"),
        };
        assert!(!allowed(&s), "{field}");
    }
    let mut s = snapshot();
    s[0].as_array_mut().unwrap().push(json!({"ifname":"eth0"}));
    assert!(!allowed(&s));
    for (family, address) in [("inet", "192.0.2.1"), ("inet6", "2001:db8::1")] {
        let mut s = snapshot();
        s[1][0]["addr_info"][0] =
            json!({"family":family,"local":address,"prefixlen":8,"scope":"host"});
        assert!(!allowed(&s));
    }
    for index in [2, 3] {
        for route in [
            json!({"dst":"default","dev":"lo","table":123}),
            json!({"dst":"192.0.2.0/24","dev":"lo","table":123}),
            json!({"dst":"2001:db8::/64","dev":"lo","table":123}),
            json!({"dst":"127.0.0.0/8","dev":"eth0"}),
            json!({"dst":"127.0.0.0/8","dev":"lo","gateway":"127.0.0.2"}),
            json!({"dst":"127.0.0.0/8","dev":"lo","nexthops":[]}),
            json!({"dst":"127.0.0.0/8","dev":"lo","encap":{}}),
        ] {
            let mut s = snapshot();
            s[index] = json!([route]);
            assert!(!allowed(&s), "route index {index}");
        }
    }
}
#[test]
fn rejects_missing_malformed_oversized_and_cross_family_snapshots() {
    for index in 0..4 {
        let s = snapshot();
        let mut bytes = s.each_ref().map(|v| serde_json::to_vec(v).unwrap());
        for bad in [b"{}".to_vec(), b"[".to_vec(), vec![b' '; 16 * 1024 + 1]] {
            bytes[index] = bad;
            assert!(validate_snapshot(bytes.each_ref().map(Vec::as_slice)).is_err());
        }
    }
    let mut s = snapshot();
    s.swap(2, 3);
    assert!(!allowed(&s));
    s = snapshot();
    s[1] = json!([]);
    assert!(!allowed(&s));
}
